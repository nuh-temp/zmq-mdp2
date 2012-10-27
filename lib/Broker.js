var util = require('util');
var events = require('events');
var zmq = require('zmq');
var debug = require('debug')(process.env.MODULE_NAME || 'broker' + process.pid);
var MDP = require('./consts');

var HEARTBEAT_LIVENESS = 3;
var HEARTBEAT_INTERVAL = 2500;
var HEARTBEAT_EXPIRY = HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL;

function Broker (endpoint, name) {
    var self = this;

    self.name = name || 'broker' + process.pid;
    self.endpoint = endpoint;

    self.services = {};
    self.workers = {};
    self.waiting = [];

    events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function (cb) {
    var self = this;

    self.socket = zmq.socket('router');
    self.socket.identity = self.name;
    self.socket.setsockopt('linger', 1);

    self.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    try {
        self.socket.bindSync(self.endpoint);
    } catch (err) {
        cb(err);
        return;
    }
    debug('Broker started on %s', self.endpoint);
    
    self.hbTimer = setInterval(function () {
        self.workerPurge();
        Object.keys(self.workers).forEach(function (identify) {
            self.socket.send([identify, MDP.WORKER, MDP.W_HEARTBEAT]);
        });
    }, HEARTBEAT_INTERVAL);
    
    cb();
};

Broker.prototype.stop = function () {
    var self = this;

    clearInterval(self.hbTimer);
    if (self.socket) {
        self.socket.close();
        delete self['socket'];
    }
};

Broker.prototype.onMsg = function (msg) {
    var self = this;

    // console.log('B: --- msg in broker ---');
    // for (var i = 0; i < msg.length; i++) {
    //     console.log('  ' + i + ': ', msg[i].toString());
    // }

    var sender = msg[0];
    var header = msg[1].toString();

    if (header === MDP.CLIENT) {
        self.onClient(msg);
    } else if (header === MDP.WORKER) {
        self.onWorker(msg);
    } else {
        self.emitErr('(onMsg) Invalid message header \'' + header + '\'');
        // send error to client
    }
};

Broker.prototype.emitErr = function (msg) {
    var self = this;

    self.emit.apply(self, ['error', msg]);
};

Broker.prototype.onClient = function (msg) {
    var self = this;

    var serviceId = msg[3].toString();
    // console.log("B: REQUEST from client: %s, service: %s", sender.toString(), serviceId);
    
    var service = self.serviceRequire(serviceId);
    self.serviceDispatch(service, msg);
};

Broker.prototype.onWorker = function (msg) {
    var self = this;

    var identify = msg[0].toString();
    var type = msg[2];
    
    var workerReady = (identify in self.workers);
    var worker = self.workerRequire(identify);

    var client;
    var serviceName;
    var i;
    var obj;
    if (type == MDP.W_READY) {
        var service = msg[3].toString();
        // console.log('B: register worker: %s, service: %s', identify, service);
        if (workerReady) {
            // Not first command in session
            self.workerDelete(identify, true);
            return;
        }
        worker.service = self.serviceRequire(service);
        worker.service.workers++;
        self.workerWaiting(worker);
    } else if (type == MDP.W_PARTIAL) {
        // console.log("B: PARTIAL from worker '%s'", identify);
        client = msg[3];
        serviceName = self.workerService(identify);
        obj = [client, MDP.CLIENT, MDP.C_PARTIAL, serviceName];
        for (i = 5; i < msg.length; i++) {
            obj.push(msg[i]);
        }
        self.socket.send(obj);
    } else if (type == MDP.W_FINAL) {
        // console.log("B: FINAL from worker '%s'", identify);
        client = msg[3];
        serviceName = self.workerService(identify);
        obj = [client, MDP.CLIENT, MDP.C_FINAL, serviceName];
        for (i = 5; i < msg.length; i++) {
            obj.push(msg[i]);
        }
        self.socket.send(obj);
        self.workerWaiting(worker);
    } else if (type == MDP.W_HEARTBEAT) {
        // console.log('B: HEARTBEAT from %s', identify);
        if (workerReady) {
            worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
        } else {
            self.workerDelete(identify, true);
        }
    } else if (type == MDP.W_DISCONNECT) {
        self.workerDelete(identify, false);
    } else {
        self.emitErr('(onMsg) Invalid message type \'' + type.toString() + '\'');
        // send error
        return;
    }
};

Broker.prototype.workerService = function (identify) {
    var self = this;

    var worker = self.workers[identify];

    return worker.service.name;
};

Broker.prototype.serviceDispatch = function (service, msg) {
    var self = this;

    if (msg) {
        service.requests.push(msg);
    }
    
    // invalidate workers list according to heartbeats
    self.workerPurge();

    while (service.waiting.length && service.requests.length) {
        // get idle worker
        var workerId = service.waiting.pop();
        var i;
        for (i = 0; i < self.waiting.length; i++) {
            if (workerId !== self.waiting[i]) {
                continue;
            }
            self.waiting.splice(i, 1);
            break;
        }

        // get next message
        var nextMsg = service.requests.pop();
        var sender = nextMsg[0].toString();
        var obj = [workerId, MDP.WORKER, MDP.W_REQUEST, sender, ''];
        for (i = 4; i < nextMsg.length; i++) {
            obj.push(nextMsg[i]);
        }
        self.socket.send(obj);
    }
    debug('serviceDispatch: requests=%s, idle=%s', service.requests.length, service.waiting.length);
};

Broker.prototype.workerPurge = function () {
    var self = this;

    for (i = 0; i < self.waiting.length; i++) {
        var identify = self.waiting[i];
        var worker = self.workers[identify];
        if ((new Date()).getTime() < worker.expiry) {
            // Worker is alive, we're done here
            break;
        }
        self.workerDelete(identify, false);
    }
};

Broker.prototype.workerWaiting = function (worker) {
    var self = this;

    // add worker to waiting list
    self.waiting.push(worker.identify);
    worker.service.waiting.push(worker.identify);

    worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
    
    // process queried messages
    self.serviceDispatch(worker.service);
};

Broker.prototype.workerRequire = function (identify) {
    var self = this;

    if (identify in self.workers) {
        return self.workers[identify];
    }

    var worker = {
        identify: identify
    };
    
    self.workers[identify] = worker;

    return worker;
};

Broker.prototype.workerDelete = function (identify, disconnect) {
    var self = this;

    debug('workerDelete \'%s\'', identify, disconnect);

    if (!(identify in self.workers)) {
        return;
    }
    var i;
    var worker = self.workers[identify];
    if (disconnect) {
        self.socket.send([identify, MDP.WORKER, MDP.W_DISCONNECT]);
    }

    // remove worker from service's waiting list
    if (worker.service) {
        for (i = 0; i < worker.service.waiting.length; i++) {
            if (identify !== worker.service.waiting[i]) {
                continue;
            }
            worker.service.waiting.splice(i, 1);
            break;
        }
        worker.service.workers--;
    }

    // remove worker from broker's waiting list
    for (i = 0; i < self.waiting.length; i++) {
        if (identify !== self.waiting[i]) {
            continue;
        }
        self.waiting.splice(i, 1);
        break;
    }
    
    // remove worker from broker's workers list
    delete self.workers[identify];
};

Broker.prototype.serviceRequire = function (name) {
    var self = this;

    if (name in self.services) {
        return self.services[name];
    }

    var service = {
        name: name,
        requests: [],
        waiting: [],
        workers: 0
    };
    
    self.services[name] = service;

    return service;
};

module.exports = Broker;

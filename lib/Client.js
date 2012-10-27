var zmq = require('zmq');
var debug = require('debug')(process.env.MODULE_NAME || 'client' + process.pid);
var util = require('util');
var events = require('events');
var MDP = require('./consts');

function Client (broker, name) {
    var self = this;

    self.name = name || 'client' + process.pid;
    self.broker = broker;

    self.callbacks = {};

    events.EventEmitter.call(this);
}
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function () {
    var self = this;

    self.socket = zmq.socket('dealer');
    self.socket.identity = self.name;
    self.socket.setsockopt('linger', 1);

    self.socket.on('message', function () {
        self.onMsg.call(self, arguments);
    });

    self.socket.connect(self.broker);
    debug('Client connected to %s', self.broker);
};

Client.prototype.stop = function () {
    var self = this;

    if (self.socket) {
        self.socket.close();
        delete self['socket'];
    }
};

Client.prototype.onMsg = function (msg) {
    var self = this;

    // console.log('C: --- reply in client ---');
    // for (var i = 0; i < msg.length; i++) {
    //     console.log('  ' + i + ': ', msg[i].toString());
    // }
    var header = msg[0].toString();
    var type = msg[1];
    if (header !== MDP.CLIENT) {
        self.emitErr('(onMsg) Invalid message header \'' + header + '\'');
        // send error
        return;
    }

    var rid = msg[3].toString();

    var callbacks = self.callbacks[rid];
    var partialCb = callbacks[0];
    var finalCb = callbacks[1];

    var data = msg[4];
    var reply;
    try {
        reply = JSON.parse(data);
    } catch (e) {
        finalCb(500, 'Unable to parse result: ' + e);
        return;
    }

    var status = reply.status;
    if (status === 200) {
        status = null;
    }
    if (type == MDP.C_PARTIAL) {
        // call partialCb
        if (partialCb) {
            partialCb(reply.msg);
        } else {
            self.emitErr('(onMsg) WARNING: Partial callback required by worker');
        }
    } else if (type == MDP.C_FINAL) {
        // call finalCb
        finalCb(status, reply.msg);
        delete self.callbacks[rid];
    } else {
        self.emitErr('(onMsg) Invalid message type \'' + type.toString() + '\'');
        // send error
        return;
    }
};

Client.prototype.emitErr = function (msg) {
    var self = this;

    self.emit.apply(self, ['error', msg]);
};

Client.prototype.request = function (service, data, partialCb, finalCb) {
    var self = this;

    if (! finalCb) {
        finalCb = partialCb;
        partialCb = undefined;
    }
    var rid = self.getRid();

    self.callbacks[rid] = [partialCb, finalCb];
    debug('send request:', rid, JSON.stringify(data));
    self.socket.send([MDP.CLIENT, MDP.C_REQUEST, service, rid, JSON.stringify(data)]);
};

Client.prototype.getRid = function () {
    var self = this;
    
    var tm = (new Date()).getTime();
    var rid = tm;
    var i = 0;
    while (rid in self.callbacks) {
        rid = tm + '.' + i++;
    }
    return rid;
};

module.exports = Client;

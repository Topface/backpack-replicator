(function(module) {
    var events    = require("events"),
        util      = require("util"),
        mess      = require("mess"),
        http      = require("http"),
        url       = require("url"),
        replicate = require("replicate-http");

    function Replicator(queue, map) {
        var self = this;

        self.map     = map;
        self.queue   = queue;
        self.ready   = self.queue.ready;
        self.started = false;

        self.queue.once("ready", function() {
            self.ready = true;
            self.emit("ready");
        });
    }

    util.inherits(Replicator, events.EventEmitter);

    Replicator.prototype.setMap = function(map) {
        this.map = map;
    };

    Replicator.prototype.start = function() {
        var self = this;

        self.started = true;

        if (self.ready) {
            start();
        } else {
            self.once("ready", start);
        }

        function start() {
            if (!self.started) {
                return;
            }

            self.emit("started");

            (function next() {
                if (!self.started) {
                    return;
                }

                self.queue.pop(function(error, item, remove, unlock) {
                    if (error) {
                        self.emit("error", error);
                        setTimeout(next, 1000);
                        return;
                    }

                    console.log("pop..", item);

                    if (!item) {
                        setTimeout(next, 1000);
                    } else {
                        self.process(JSON.parse(item), function(error) {
                            if (error) {
                                self.emit("error", error);
                                unlock(function() {
                                    setTimeout(next, 1000);
                                });
                                return;
                            }

                            remove(function() {
                                setTimeout(next, 0);
                            });
                        });
                    }
                });
            })();
        }
    };

    Replicator.prototype.process = function(item, callback) {
        item.from = mess(item.from).pop();

        if (!this.map[item.from]) {
            callback(new Error("Unknown server id: " + item.from));
            return;
        }

        if (!this.map[item.to]) {
            callback(new Error("Unknown server id: " + item.to));
            return;
        }

        this.replicate(item.from, item.to, item.path, callback)
    };

    Replicator.prototype.replicate = function(from, to, path, callback) {
        replicate(this.map[item.from] + item.path, this.map[item.to] + item.path, callback);
    };

    Replicator.prototype.push = function(from, to, path, callback) {
        var self = this,
            ok   = true;

        from.forEach(function(id) {
            if (!ok) {
                return;
            }

            if (!self.map[id]) {
                callback(new Error("Unknown server id: " + id));
                ok = false;
            }
        });

        if (!ok) {
            return;
        }

        if (!self.map[to]) {
            callback(new Error("Unknown server id: " + to));
            return;
        }

        self.queue.push(JSON.stringify({
            from : from,
            to   : to,
            path : path
        }), callback);
    };

    Replicator.prototype.stop = function() {
        this.started = false;
        setTimeout(this.emit.bind(this, "stopped"));
    };

    module.exports = Replicator;
})(module);

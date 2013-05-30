(function(module) {
    var Replicator = require("../index"),
        redis      = require("redis"),
        Zk         = require("zkjs"),
        Queue      = require("zk-redis-queue"),
        events     = require("events"),
        util       = require("util"),
        nop        = require("nop"),
        async      = require("async");

    /**
     * Looks for zk configuration changes and auto-updates replicator.
     */
    function Supervisor(zk, concurrency) {
        this.zk         = zk;
        this.replicator = undefined;
        this.closed     = false;

        this.concurrency = concurrency || 1;
        this.servers_map = undefined;
        this.queue       = undefined;
    }

    util.inherits(Supervisor, events.EventEmitter);

    Supervisor.prototype.getReplicator = function(callback) {
        var self = this;

        if (self.closed) {
            callback(new Error("Trying to get replicator from closed supervisor"));
            return;
        }

        if (self.replicator) {
            callback(undefined, this.replicator);
            return;
        }

        function zkRestart() {
            if (self.closed) {
                return;
            }

            self.zk.start(function(error) {
                if (error) {
                    self.emit("error", error);
                    setTimeout(zkRestart, 1000);
                } else {
                    (function reloadConfiguration() {
                        var requests = {};

                        requests.servers_map = self.subscribeForServersMapUpdates.bind(self);
                        requests.queue       = self.subscribeForQueueInfoUpdates.bind(self);

                        async.parallel(requests, function(error) {
                            if (error) {
                                // no errors could happen here, we retry while we can
                                // let's throw it so everyone will know
                                throw error;
                            }

                            self.replicator = new Replicator(self.queue, self.servers_map, self.concurrency);

                            callback(undefined, self.replicator);
                        });
                    })();
                }
            });
        }

        self.zk.on("expired", zkRestart);

        zkRestart();
    };

    Supervisor.prototype.subscribeForServersMapUpdates = function(callback) {
        var self = this,
            watcher;

        if (self.closed) {
            return;
        }

        watcher = self.subscribeForServersMapUpdates.bind(self, nop);

        self.zk.get("/servers-map", watcher, function(error, map) {
            if (error) {
                self.emit("error", error);
                setTimeout(self.subscribeForServersMapUpdates.bind(self, callback), 1000);
                return;
            }

            self.servers_map = JSON.parse(map);

            if (self.replicator) {
                self.replicator.setMap(self.servers_map);
            }

            callback();
        });
    };

    Supervisor.prototype.subscribeForQueueInfoUpdates = function(callback) {
        var self = this,
            watcher;

        if (self.closed) {
            return;
        }

        watcher = self.subscribeForQueueInfoUpdates.bind(self, nop);

        self.zk.get("/queue", watcher, function(error, queue) {
            if (error) {
                self.emit("error", error);
                setTimeout(self.subscribeForQueueInfoUpdates.bind(self, callback), 1000);
                return;
            }

            queue = JSON.parse(queue);

            function redises(hosts) {
                return hosts.map(function(config) {
                    return redis.createClient(config.port, config.host, {retry_max_delay: 1000});
                });
            }

            queue = new Queue(redises(queue.servers), new Zk(self.zk.options), queue.key);
            queue.on("error", self.emit.bind(self, "error"));
            queue.once("ready", function() {
                if (self.replicator && self.replicator.queue) {
                    self.replicator.queue.close();
                }

                self.queue = queue;

                if (self.replicator) {
                    self.replicator.setQueue(queue);
                }

                callback();
            });
        });
    };

    Supervisor.prototype.stop = function() {
        var self = this;

        if (self.closed) {
            return;
        }

        self.closed = true;

        self.zk.close();
    };

    module.exports = Supervisor;
})(module);

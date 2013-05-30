(function(module) {
    var events    = require("events"),
        util      = require("util"),
        mess      = require("mess"),
        http      = require("http"),
        url       = require("url"),
        replicate = require("replicate-http");

    /**
     * Create replicator with given level of concurrency.
     *
     * @param {Queue} queue Queue instance of zk-redis-queue
     * @param {Object} map Map node_id -> node_info (with host and port keys)
     * @param {Number|undefined} concurrency Concurrency level (1 if undefined)
     * @constructor
     */
    function Replicator(queue, map, concurrency) {
        this.started     = false;
        this.concurrency = concurrency || 1;
        this.processing  = 0;

        this.setMap(map);
        this.setQueue(queue);
    }

    util.inherits(Replicator, events.EventEmitter);

    /**
     * Update servers map.
     *
     * @param {Object} map Map node_id -> node_info (with host and port keys)
     */
    Replicator.prototype.setMap = function(map) {
        this.map = map;
    };

    /**
     * Update queue object, previous queue object must be utilized manually.
     *
     * @param {Queue} queue Queue instance of zk-redis-queue
     */
    Replicator.prototype.setQueue = function(queue) {
        var self = this;

        self.queue = queue;
        self.ready = self.queue.ready;

        if (!self.ready) {
            self.queue.once("ready", function() {
                if (self.queue != queue) {
                    return;
                }

                self.ready = true;
                self.emit("ready");
            });
        }
    };

    /**
     * Start replication once queue is ready.
     */
    Replicator.prototype.start = function() {
        this.started = true;

        if (this.ready) {
            this.spawn();
        } else {
            this.once("ready", this.spawn.bind(this));
        }
    };

    /**
     * Spawn workers to replicate, called from start.
     */
    Replicator.prototype.spawn = function() {
        if (!this.started) {
            return;
        }

        this.emit("started");

        while (this.processing < this.concurrency) {
            this.processing++;
            this.next();
        }
    };

    /**
     * Process next item if started, calls itself on finish.
     */
    Replicator.prototype.next = function() {
        var self = this,
            next = self.next.bind(self);

        if (!self.started) {
            self.processing--;
            if (!self.processing) {
                self.emit("stopped");
            }

            return;
        }

        self.queue.pop(function(error, item, remove, unlock) {
            if (error) {
                self.emit("error", error);
                setTimeout(next, 1000);
                return;
            }

            console.log(Date.now(), "pop..");

            if (!item) {
                setTimeout(next, 1000);
            } else {
                self.process(JSON.parse(item), function(error) {
                    if (error) {
                        self.emit("error", error);

                        unlock(function(error) {
                            if (error) {
                                self.emit("error", error);
                            }

                            setTimeout(next, 1000);
                        });
                    }

                    remove(function(error) {
                        if (error) {
                            self.emit("error", error);
                        }

                        setTimeout(next, 0);
                    });
                });
            }
        });
    };

    /**
     * Queue item processing.
     *
     * @param {Object} item Queue item to process
     * @param {Function} callback Callback to call on finish
     */
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

    /**
     * Actually replicate data from -> to on given path.
     *
     * @param {Object} from Source node id
     * @param {Object} to Destination node id
     * @param {String} path Path to replicate
     * @param {Function} callback Callback to call on finish
     */
    Replicator.prototype.replicate = function(from, to, path, callback) {
        console.log(this.getUrl(from, path), "->", this.getUrl(to, path));
        replicate(this.getUrl(from, path), this.getUrl(to, path), callback);
    };

    /**
     * Returns url by node id and path
     *
     * @param {Number} id Node id
     * @param {String} path Path on node
     * @returns string
     */
    Replicator.prototype.getUrl = function(id, path) {
        var format = {};

        format.hostname = this.map[id].host;
        format.port     = this.map[id].port;

        format.protocol = 'http:';
        format.pathname = path;

        return url.format(format);
    };

    /**
     * Push replication task to queue.
     *
     * @param {Array} from Array of source node ids to download from
     * @param {Number} to Destination node id to replicate to
     * @param {String} path Path to replicate
     * @param callback
     */
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

    /**
     * Stop replication tasks, will emit "stopped" event.
     */
    Replicator.prototype.stop = function() {
        this.started = false;

        if (!this.processing) {
            setTimeout(this.emit.bind(this, "stopped"), 0);
        }
    };

    module.exports = Replicator;
})(module);

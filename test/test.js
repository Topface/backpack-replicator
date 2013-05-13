(function() {
    var assert     = require("assert"),
        async      = require("async"),
        Redis      = require("redis"),
        Queue      = require("zk-redis-queue"),
        Replicator = require("../index.js"),
        ZK         = require("zkjs"),
        zk         = new ZK({hosts: ["api.yongwo.de:2181"], root: "/boo", timeout: 2000}),
        redisOne   = Redis.createClient(6379, "127.0.0.1", {retry_max_delay: 1000}),
        redisTwo   = Redis.createClient(6380, "127.0.0.1", {retry_max_delay: 1000}),
        queue      = new Queue([redisOne, redisTwo], zk, "boo"),
        map        = {1: "http://google.com", 2: "http://yandex.ru"},
        replicator = new Replicator(queue, map);

    replicator.replicate = function(from, to, path, callback) {
        console.log("replicating", this.map[from] + path, "->", this.map[to] + path);
        callback();
    }

    queue.on("error", function(error) {
        console.log("Got queue error", error);
    });

    replicator.on("ready", function() {
        console.log("Replicator is ready");

        async.series([
            replicator.push.bind(replicator, [1], 2, "/lol"),
            replicator.push.bind(replicator, [2], 1, "/wtf")
        ], function(error) {
            assert.ifError(error);

            replicator.push([2], 3, "/undefined", function(error) {
                assert.ok(error);
            });

            replicator.push([3], 2, "/noooo", function(error) {
                assert.ok(error);
            });

            queue.getSize(function(error, size) {
                assert.ifError(error);
                assert.equal(size, 2);

                console.log('Starting replicator');
                replicator.start();

                setTimeout(function() {
                    replicator.stop();

                    replicator.on("stopped", function() {
                        assert.equal(replicator.started, false);

                        console.log("Replicator stopped");

                        queue.getSize(function(error, size) {
                            assert.ifError(error);
                            assert.equal(size, 0);

                            end();
                        });
                    });
                }, 10000);
            });
        });
    });

    replicator.on("error", function(error) {
        console.log("Replicator error occurred", error);
    });

    function end(error) {
        assert.ifError(error);

        console.log("All done!");

        redisOne.quit();
        redisTwo.quit();
        zk.close();
    }
})();

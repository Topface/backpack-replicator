#!/usr/bin/env node
(function() {
    var Replicator = require("../index"),
        Zk         = require("zkjs");

    module.exports.start = function() {
        var args = process.argv.slice(2),
            zk, supervisor;

        if (args.length < 2) {
            console.error("Usage: backpack-replicator <zk_servers> </zk/root> [concurrency]");
            process.exit(1);
        }

        zk         = new Zk({hosts: args[0].split(","), root: args[1]});
        supervisor = new Replicator.Supervisor(zk, args[2]);

        supervisor.on("error", console.error.bind(console));

        supervisor.getReplicator(function(error, replicator) {
            if (error) {
                throw error;
            }

            replicator.on("error", console.error.bind(console));
            replicator.on("ready", console.log.bind(console, "Replicator is ready!"));
            replicator.on("started", console.log.bind(console, "Replicator started!"));

            replicator.start();
        });
    };
})();

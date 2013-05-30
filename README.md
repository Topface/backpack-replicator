backpack-replicator
====

Reliably replicate items between instances, runs as daemon that auto-updates itself configuration.
Reads queue from [backpack-coordinator](https://github.com/Topface/backpack-coordinator) and does what it says.

### Installation

```
npm backpack-replicator
```

### Usage

Use values from your [backpack-coordinator](https://github.com/Topface/backpack-coordinator).

```
Usage: backpack-replicator <zk_servers> </zk/root> [concurrency]
```

`concurrency` is the number of items that replicator will process in parallel.

### Configuration

Initial configuration is stored in zookeeper. When zookeeper data changes, backpack-replicator
automatically reload and apply new configuration so there's no need for manual restarts.

### Authors

* [Ian Babrou](https://github.com/bobrik)

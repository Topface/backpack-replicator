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
Usage: backpack-replicator <zk_servers> </zk/root> <redis_servers> <redis_key>
```

### Authors

* [Ian Babrou](https://github.com/bobrik)

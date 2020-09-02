# Let Me Schedule Tasks For You (lmstfy)
[![Build Status](https://travis-ci.org/bitleak/lmstfy.svg?branch=master)](https://travis-ci.org/bitleak/lmstfy) [![Go Report Card](https://goreportcard.com/badge/github.com/bitleak/lmstfy)](https://goreportcard.com/report/github.com/bitleak/lmstfy) [![Coverage Status](https://coveralls.io/repos/github/bitleak/lmstfy/badge.svg?branch=add-coverage-reports)](https://coveralls.io/github/bitleak/lmstfy?branch=add-coverage-reports) [![GitHub release](https://img.shields.io/github/tag/bitleak/lmstfy.svg?label=release)](https://github.com/bitleak/lmstfy/releases) [![GitHub release date](https://img.shields.io/github/release-date/bitleak/lmstfy.svg)](https://github.com/bitleak/lmstfy/releases) [![LICENSE](https://img.shields.io/github/license/bitleak/lmstfy.svg)](https://github.com/bitleak/lmstfy/blob/master/LICENSE) [![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/bitleak/lmstfy)

lmstfy(pronounce /'lam.si.fai/) is a simple task queue (or job queue) service, providing the following features:

- basic job queue primitives: PUBLISH, CONSUME and DELETE via HTTP API
- support extra lifecycle management of jobs:
    * job TTL (time-to-live)
    * job delay trigger (at second granularity)
    * job auto-retry
    * dead letter
- namespace/queue level metrics
- token consume/produce rate limit

lmstfy itself doesn't handle data storage, it delegates the storage to the `Redis` or `Redis Sentinel` currently (a file based
storage backend is under implementing). So data integrity and durability is in the hand of redis,
we use AOF and replication on our production env to ensure that.

## Who use lmstfy 

<table>
<tr>
<td height = "128" width = "164"><img src="https://imgur.com/9X1kc2j.png" alt="Meitu"></td>
</tr>
</table>

## SDK for lmstfy

* [php lmstfy client](https://github.com/bitleak/php-lmstfy-client)
* [golang lmstfy client](https://github.com/bitleak/lmstfy/tree/master/client)
* [java lmstfy client](https://github.com/bitleak/java-lmstfy-client)
* [rust lmstfy client](https://github.com/bitleak/rust-lmstfy-client)

## Build and Run

To build the server binary:
```
$ make # target file would be inside _build dir
```

To run the server:
```
redis-server &
./_build/lmstfy-server -c config/demo-conf.toml
```

To start in developing mode:
```
./_build/lmstfy-server -c config/demo-conf.toml -bt debug -sv
```

**You can use `./scripts/token-cli` to manage the namespace and token**

## DOCs

* [HTTP API Doc](https://github.com/bitleak/lmstfy/blob/master/doc/API.md)
* [Administration API Doc](https://github.com/bitleak/lmstfy/blob/master/doc/administration.en.md)
* [Throtter API Doc](https://github.com/bitleak/lmstfy/blob/master/doc/throtter.en.md)
* [Pusher API Doc](https://github.com/bitleak/lmstfy/blob/master/doc/pusher.en.md)
* [管理 API 中文文档](https://github.com/bitleak/lmstfy/blob/master/doc/administration.cn.md)
* [限流 API 中文文档](https://github.com/bitleak/lmstfy/blob/master/doc/throtter.cn.md)
* [推送 API 中文文档](https://github.com/bitleak/lmstfy/blob/master/doc/pusher.cn.md)

---

## Dashboard

* [Grafana](https://grafana.com/grafana/dashboards/12748)

## Internal

Detailed internal implementation looks like:

<img src="https://github.com/bitleak/lmstfy/raw/master/doc/job-flow.png" alt="job flow" width="800px">

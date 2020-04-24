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

**You can use the `./scripts/token-cli` to manage the namesace and the token**

## HTTP API

From a thousand miles above, the main work flow looks like:

    some piece of job data(message) is sent and some time later
    it gets picked up by a worker that is listening on the queue.

When the worker accomplish what the job message told it to
do, the worker delete the job from the queue to mark it as
finished.

If the worker died somehow, the job will be picked up by
other worker that is alive after the configured TTR
(time-to-run) time.

```

PUBLISH  ---> [[ queue ]] ---> CONSUME ---done--> DELETE
                  ^                     |
                  |                   failed
                  |                     |
                  +---------------------+

```

There is no API for creating queue, queues of the namespace are
created on the go! You can publish msg to any queue under the
namespace that you're authorized to publish to.

To access the HTTP API, you need to get a `token` for your
namespace from the administrators. And use the `token` to access
any of the following API.

### Common Request Parameters

The `token` can be provided in two ways:

1. Add a header `X-Token` with the `token` as value to all the API requests
2. Add a url query `token=XXXX` to all your API requests

Either way would work, suit your fit.

> All time related parameters are in seconds.  

The limits on parameters:

1. Namespace and Queue name must be smaller than 256
2. TTL, TTR, Delay and Timeout must be `<=` uint32.MAX
3. Job body must be smaller than 64kB
4. Tries must be `<=` uint16.MAX


### Common Response Headers

Every response will have a `X-Request-ID` header set, the value is the unique
ID identifying the request. User might log this header to assist debugging.

---

### Publish a job

```
PUT /api/:namespace/:queue
```

#### Request Query

- delay: number of seconds to delay  (default: 0)
- ttl:   time-to-live of this job in seconds, 0 means forever.  (default: 24 hours)
- tries: number of attempts shall this job be consumed  (default: 1)

NOTE: `tries` can be used to ensure that a job will be retried when worker that consumed
the job, failed to invoke the DELETE req. eg. setting `tries=2` means if the worker
failed, it will be retried one more time after the TTR.

#### Request Body

The job content is any text/binary that can be sent over HTTP.
The content length must be *SMALLER* than 64kB.

#### Response JSON Body

-   201

    ```
    {
        "msg": "published",
        "job_id": string
    }
    ```

-   400

    ```
    {
        "error": string
    }
    ```

-   413

    ```
    {
        "error": "body too large"
    }
    ```

---

### Consume a job

```
GET /api/:namespace/:queue[,:queue]*
```

You can consume multiple queues of the same namespace at once,
just concat the queues with comma as separation. eg.

```
GET /api/myns/q1,q2,q3?timeout=5
```

Request above will consume from q1, q2 and q3. The order implies
the priority, that is if all queues have jobs available, the job
in q1 will be return first, and so on.

NOTE: to consume multiple queues, `timeout` (seconds) must be specified.

#### Request Query

- timeout: the longest to time to wait before a job is available (default: 0)
- ttr:     time-to-run of the job in seconds, if the job is not delete in this period, it will be re-queued  (default: 2 minutes)
- count: how many jobs expect to be consumed (default: 1)

#### Response

-   200

    ```
    {
        "msg": "new job",
        "namespace": string,
        "queue": string,
        "job_id": string,
        "data": string    # NOTE: the data is base64 encoded, you need to decode
        "ttl": int64
        "elapsed_ms": int64  # elapsed milliseconds since published
        "remain_tries": int64  # remain retry time
    }
    
    or 
    [ // batch consume
        {
        "msg": "new job",
        "namespace": string,
        "queue": string,
        "job_id": string,
        "data": string    # NOTE: the data is base64 encoded, you need to decode
        "ttl": int64
        "elapsed_ms": int64  # elapsed milliseconds since published
        "remain_tries": int64  # remain retry time
       }
       ...
    ]
    ```

-   400
  
    ```
    {
        "error": string
    }
    ```

-   404
  
    ```
    {
        "msg": "no job available"
    }
    ```

---

### Delete a job (ACK)

```
DELETE /api/:namespace/:queue/job/:job_id
```

Delete makes sure that this job will never be consumed again disregarding the job's `tries`

#### Response

-   204

    nil body

-   400

    ```
    {
        "error": string
    }
    ```

### Peek a job from queue (get job data without consuming it)

```
GET /api/:namespace/:queue/peek
```

NOTE: Peek will return the job with ttl 0 and null data if its ttl expired.

#### Response

-   200

    ```
    {
        "namespace": string,
        "queue": string,
        "job_id": string,
        "data": string    # NOTE: the data is base64 encoded, you need to decode
        "ttl": int64
        "elapsed_ms": int64
        "remain_tries": int64    # remain retry time
    }
    ```

-   404

    ```
    {
        "error": "the queue is empty"
    }
    ```

---

### Peek a specific job of `job_id`

```
GET /api/:namespace/:queue/job/:job_id
```

NOTE: Peek can't return the job data if its ttl expired.

#### Response

-   200

    ```
    {
        "namespace": string,
        "queue": string,
        "job_id": string,
        "data": string    # NOTE: the data is base64 encoded, you need to decode
        "ttl": int64
        "elapsed_ms": int64
        "remain_tries": int64    # remain retry time
    }
    ```

-   404
  
    ```
    {
        "error": "job not found"
    }
    ```

---

### Query the queue size (how many jobs are ready to be consumed)

```
GET /api/:namespace/:queue/size
```

#### Response

-   200

    ```
    {
        "namespace": string,
        "queue": string,
        "size": int,
    }
    ```

---

### Destroy queue (DANGER ! look out)

```
DELETE /api/:namespace/:queue
```

Delete all the jobs that's in the ready queue, but jobs in the delayed state and working state are
not deleted.

#### Response

-   204

    nil body

---

## Dead Letter API

When a job failed to ACK within `ttr` and no more tries are available, the job will be put
into a sink called "dead letter". Job in dead letter can be respawned and consumed by worker again,
or it can just be deleted.

> NOTE 1: if the job has been deleted, respawning that job is a vain effort.

> NOTE 2: all respawned jobs are configured with `ttl=86400&tries=1&delay=0` by default.

> NOTE 3: when put into dead letter, the job's TTL is removed. That means all jobs in deadletter will
not expire. and You can configure new TTL again when respawning the job.

### Peek the dead letter

```
GET /api/:namespace/:queue/deadletter
```

#### Response

-   200
    ```
    {
        "namespace": string,
        "queue": string,
        "deadletter_size": int,
        "deadletter_head": string,  # the first dead job ID in the dead letter
    }
    ```

---

### Respawn job(s) in the dead letter

```
PUT /api/:namespace/:queue/deadletter
```

#### Request Query

- limit: the number (upper limit) of the jobs to be respawned  (default 1).
- ttl:   time-to-live of this job in seconds, 0 means forever.  (default: 24 hours)

#### Response

-   200
    ```
    {
        "msg": "respawned",
        "count": int,   # the number of jobs that're respawned
    }
    ```

---

### Delete job(s) in the dead letter

```
DELETE /:namespace/:queue/deadletter
```

#### Request Query

- limit: the number (upper limit) of the jobs to be deleted (default 1).

#### Response

-   204

    nil body

### Query the deadletter queue size (how many jobs are reached the max retry times)

```
GET /api/:namespace/:queue/deadletter/size
```

#### Response

-   200

    ```
    {
        "namespace": string,
        "queue": string,
        "size": int,
    }
    ```

---

## Internal

Detailed internal implementation looks like:

<img src="https://github.com/bitleak/lmstfy/raw/master/doc/job-flow.png" alt="job flow" width="800px">

## Pusher API

Pusher admin api config the pusher configuration on pool-namespace-group

> CAUTION: consideration of the performance, we sync pusher config every 3 seconds instead of fetching the config every time.

### Create Pusher

```
POST /pusher/:namespace/:group
```

#### Request Body 

```
{
    "queues": ["high_prio_queue", "normal_prio_queue", "low_prio_queue"]
    "endpoint": "http://endpoint-url",
    "workers": 10,
    "timeout": 3
}
```

The unit of `timeout` is second and, `workers` is the number of pusher processes，`endpoint`is a full http network address, Lmstfy will Post this endpoint with body, like:

```$json
{
    "namespace": "test-namespace",
    "queue": "high_prio_queue",
    "id": "job_id",
    "ttl": 10,
    "elapsed_ms": 15842,
    "body": "dGVzdC1ib2R5" // base64-encoded
}
```

### List All Pusher

```
GET /pushers
```

#### Request Query

no parameter

#### Response Body

```
{
    "pushers": {
        "default": {
            "test-namespace": [
                "test-queue"
            ]
        }
    }
}
```

### List All Pusher of a namespace

```
GET /pusher/:namespace
```

#### Request Query

- pool: must, pool name

#### Response Body

```
{
    "pushers": {
        "test-queue": {
            "queues": ["high_prio_queue", "normal_prio_queue", "low_prio_queue"],
            "endpoint": "http://0.0.0.0:9090",
            "workers": 10,
            "timeout": 3
        }
    }
}
```

### Get push group

```
GET /pusher/:namespace/:group
```

#### Request Query

- pool: optional, pool name，default is `default`

#### Response Body

```
{
    "pusher": {
        "queues": ["high_prio_queue", "normal_prio_queue", "low_prio_queue"],
        "endpoint": "http://0.0.0.0:9090",
        "workers": 10,
        "timeout": 3
    }
}
```


### Update a Pusher

```
PUT /pusher/:namespace/:queue
```

#### Request Query

- pool: optional, pool name，default is `default`

#### Request Body 

```
{
    "queues": ["high_prio_queue", "normal_prio_queue", "low_prio_queue"]
    "endpoint": "http://endpoint-url",
    "workers": 10,
    "timeout": 3
}
```

### Delete a Pusher

```
DELETE /pusher/:namespace/:group
```

#### Request Query

- pool: optional, pool name，default is `default`



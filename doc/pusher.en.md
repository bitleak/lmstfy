## Pusher API

Pusher admin api config the pusher configuration on pool-namespace-queue

> CAUTION: consideration of the performance, we sync pusher config every 3 seconds instead of fetching the config every time.

### Create Pusher

```
POST /pusher/:namespace/:queue
```

#### Request Body 

```
{
    "endpoint": "http://endpoint-url",
    "workers": 10,
    "timeout": 3
}
```

The unit of `timeout` is second and, `workers` is the number of pusher processes，`endpoint`is a full http network address, Lmstfy will Post this endpoint with body, like:

```$json
{
    "namespace": "test-namespace",
    "queue": "test-queue",
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
            "endpoint": "http://0.0.0.0:9090",
            "workers": 10,
            "timeout": 3
        }
    }
}
```

### List the Pusher of a queue 

```
GET /pusher/:namespace/:queue
```

#### Request Query

- pool: optional, pool name，default is `default`

#### Response Body

```
{
    "pusher": {
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
    "endpoint": "http://endpoint-url",
    "workers": 10,
    "timeout": 3
}
```

### Delete a Pusher

```
DELETE /pusher/:namespace/:queue
```

#### Request Query

- pool: optional, pool name，default is `default`



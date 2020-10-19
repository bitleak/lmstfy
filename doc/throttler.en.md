## Throttler API
The throttler only limits the rate of token consume/publish QPS instead of messages, don't use batch consume if you want to limit the rate of message.
Consume/Produce API would return the status code `429`(too many requests) when the token has reached the rate limit.

> CAUTION: consideration of the performance, we sync limiters every 10 seconds instead of fetching the limit every time. 

### Create the limit

```
POST /token/:namespace/:token/limit
POST /token/:namespace/:token/limit/:queue
```
#### Request Body 

```
{
"read": 100,
"write": 200,
"interval": 10,
"forbid_read":false,
"forbid_write":false
}
```

The unit of the `interval` is second and `read`/`write` is counter, which means this token can consume 100 times
and publish 200 times at 10 seconds.
`forbid_read` set true to pause consume，`forbid_write` set true to pause publish

### Get the limit

```
GET /token/:namespace/:token/limit
GET /token/:namespace/:token/limit/:queue
```

#### Request Query

no parameter

### Set the limit

```
PUT /token/:namespace/:token/limit
PUT /token/:namespace/:token/limit/:queue
```
#### Request Body 

```
{
"read": 200,
"write": 400,
"interval": 10,
"forbid_read":false,
"forbid_write":false,
}
```

### Delete the limit

```
DELETE /token/:namespace/:token/limit
DELETE /token/:namespace/:token/limit/:queue
```

#### Request Query

no parameter

### List the limit

```
GET /limits
```

#### Request Query

no parameter

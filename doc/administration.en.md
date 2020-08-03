## Admin API

### List Pool 

```
GET /pools/
```

#### Request Query

no parameter

### Create Token 

```
POST /token/:namespace
```

#### Request Query

- description: description of the token
- pool: optional, Default: "default"


### DELETE Token

```
DELETE /token/:namespace/:token
```

#### Request Query
- pool: optional, Default: "default"


### List Token 

```
GET /token/:namespace
```

#### Request Query
- pool: optional, Default: "default"

### List Namespace And Queue

```
GET /info
```

#### Request Query
- pool: optional, 默认 "default"

### Get Prometheus Metrics 

```
GET /metrics
```

#### Request Query

no parameter

## Migrate Redis

Assume the old pool (default) config was below:

```
[Pool]
[Pool.default]
Addr = "localhost:6379"
```

would migrate to new redis, just add the new configuration:

```
[Pool]
[Pool.default]
Addr = "localhost:6379"
MigrateTo = "migrate"

[Pool.migrate]
Addr = "localhost:6389"
```

New write requests would redirect to the new pool and read reqeusts would try to consume the old pool first. We
can remove the old pool configuration if the redis key and queue size wasn't changed, and recofingure the pool as below:

```
[Pool]
[Pool.default]
Addr = "localhost:6389"  # use new redis after migrated 
```

## Pusher API

任务推送管理API针对pool-namespace-group级别来配置任务推送

> 注意:为了性能考虑，推送配置目前是异步每 3s 更新一次，增加或者删除需要等待异步更新才会生效

### 创建推送

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

`timeout` 的单位是 秒，`workers` 是推送线程个数，`endpoint`是完整的任务推送地址，lmstfy会使用Post方法调用endpoint。请求如下：

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

### 列出所有配置的推送 

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

### 列出某个namespace下配置的推送 

```
GET /pusher/:namespace
```

#### Request Query

- pool: 必须, pool名称

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

### 列出某个queue配置的推送 

```
GET /pusher/:namespace/:group
```

#### Request Query

- pool: 非必须, pool名称，默认为default

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


### 更新推送配置

```
PUT /pusher/:namespace/:group
```

#### Request Query

- pool: 非必须, pool名称，默认为default

#### Request Body 

```
{
    "queues": ["high_prio_queue", "normal_prio_queue", "low_prio_queue"],
    "endpoint": "http://endpoint-url",
    "workers": 10,
    "timeout": 3
}
```

### 删除推送 

```
DELETE /pusher/:namespace/:group
```

#### Request Query

- pool: 非必须, pool名称，默认为default



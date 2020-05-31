## Throttler API

限流目前是针对 token 级别来限制消费/写入 QPS 而不是消息数，如果希望限制消息数，那么就不要使用批量消费 API 即可，当消费/写入达到限制频率时接口会返回 `429` 的状态码。

> 注意:为了性能考虑，限制阀值目前是异步每 10s 更新一次，增加或者删除不会需要等待异步更新才会生效

### 创建限速器 

```
POST /token/:namespace/:token/limit
```

#### Request Body 

```
{
"read": 100,
"write": 200,
"interval": 10
}
```

`interval` 的单位是 秒，`read/write` 的单位是次数，上面的意思是这个 token 在 10s 之内最多可以消费 100 次以及写入 200 次。

### 查看限制器 

```
GET /token/:namespace/:token/limit
```

#### Request Query

no parameter

### 设置限制器

```
PUT /token/:namespace/:token/limit
```
#### Request Body 

```
{
"read": 200,
"write": 400,
"interval": 10
}
```

### 删除限制器 

```
DELETE /token/:namespace/:token/limit
```

#### Request Query

no parameter

### 罗列限制器
```
GET /limits
```

#### Request Query

no parameter
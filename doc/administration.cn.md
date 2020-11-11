## Admin API

admin api 权限管理通过在配置文件里面增加 basic 认证的账号列表, 如果未配置则不开启 basic 认证

### 查看 pool 列表

```
GET /pools/
```

#### Request Query

不需要参数

### 创建 `token`

```
POST /token/:namespace
```

#### Request Query

- description: 创建 token 的用途描述
- pool: optional, 默认 "default"


### 删除 `token`

```
DELETE /token/:namespace/:token
```

#### Request Query
- pool: optional, 默认 "default"


### 列出所有 `token`

```
GET /token/:namespace
```

#### Request Query
- pool: optional, 默认 "default"

### 列出所有 `namespace` 和 `queue`

```
GET /info
```

#### Request Query
- pool: optional, 默认 "default"

### 获取 prometheus 监控指标 

```
GET /metrics
```

#### Request Query

不需要参数

## 迁移 redis

对于假设旧的 pool (default) 配置如下:

```
[Pool]
[Pool.default]
Addr = "localhost:6379"
```

迁移到新的 redis 只需要更改配置为:

```
[Pool]
[Pool.default]
Addr = "localhost:6379"
MigrateTo = "migrate"

[Pool.migrate]
Addr = "localhost:6389"
```

用户的 token 不需要改变, 所有写操作到会写到新的 pool; 所有读操作会优先读取旧的 pool 的内容.

等到旧的 pool 的 redis key 数量和队列 size 不在变化了. 就可以下掉, 更新配置如下:

```
[Pool]
[Pool.default]
Addr = "localhost:6389"  # 数据迁移结束后, 直接使用新的 redis 即可.
```

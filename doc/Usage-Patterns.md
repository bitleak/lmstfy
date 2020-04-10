# Common Usage Patterns for lmstfy

## 普通的异步任务执行 (Fire and Forget)

如下图所示, 任务发送到相应的队列后, 所有的 worker 作为计算集群竞争地获取相应的任务. 抢到任务
的 worker 在执行完相应的任务后, 删除这个任务. 如果期间 worker 异常死亡了, 而且如果任务
配置了重试次数, 那其他剩下的 worker 在一段时间后(TTR), 会重新去获取任务.

<img src="https://github.com/bitleak/lmstfy/raw/master/doc/fire-forget.png" alt="fire and forget" width="800px">

涉及到的 API demo:

### publisher

```
# 发布任务, (如果任务失败, 最多重试两次)
curl -X PUT -H "X-Token: xxx" "localhost:9999/api/test/q?tries=3" -d "hey new job"
```

### worker

worker 的工作流循环:

```
# 获取任务
curl -H "X-Token: xxx" "localhost:9999/api/test/q?timeout=10" > job.json

# 处理任务
cat job.json

# 删除完成的任务 (job_id 为获得到的任务的 ID)
curl -X DELETE -H "X-Token: xxx" "localhost:9999/api/test/q/job/{job_id}"

# 回到前面, 继续获取任务
```

## 延迟任务 (Delay and Fire)

和上面的异步任务类似, 但是 publisher 可以指定任务在过了一段时间之后才能被 worker 发现.

<img src="https://github.com/bitleak/lmstfy/raw/master/doc/delay-fire.png" alt="delay and fire" width="800px">

### publisher

```
# 发布任务, (如果任务失败, 最多重试两次, 延迟 30 秒)
curl -X PUT -H "X-Token: xxx" "localhost:9999/api/test/q?tries=3&delay=30" -d "hey new delay job"
```


## "同步非阻塞" (Fire and Wait, 这其实是一种不是很"高效"的 RPC)

publisher 发布任务后需要等待任务的返回结果. 这个使用的场景和使用 RPC 的场景类似, 不同的是, publisher 不需要知道
任何 worker 相关的信息, 也不需要检查 worker 的健康状态, worker 是可以水平扩展的.

<img src="https://github.com/bitleak/lmstfy/raw/master/doc/fire-wait.png" alt="fire and wait" width="800px">

### publisher

```
# 发布任务, (如果任务失败, 最多重试两次)
curl -X PUT -H "X-Token: xxx" "localhost:9999/api/test/q?tries=3" -d "hey new job"

# 等待任务执行成功的通知, (其实就是等待一个临时的任务队列, 队列名字和发布的任务的 ID 相同)
curl -H "X-Token: xxx" "localhost:9999/api/test/{job_id}"
```

### worker

worker 的工作流循环:

```
# 获取任务
curl -H "X-Token: xxx" "localhost:9999/api/test/q?timeout=10" > job.json

# 处理任务
cat job.json

# 通知 publisher 任务执行成功
curl -X PUT -H "X-Token: xxx" "localhost:9999/api/test/{job_id}" -d "hey I'm done"

# 删除完成的任务 (job_id 为获得到的任务的 ID)
curl -X DELETE -H "X-Token: xxx" "localhost:9999/api/test/q/job/{job_id}"

# 回到前面, 继续获取任务
```

## 优先级任务队列

worker 可以一次监听多个任务队列, 并根据监听的顺序, 优先获取排在前面的任务.

### worker

```
# 获取任务
curl -H "X-Token: xxx" "localhost:9999/api/test/q1,q2,q3?timeout=10" > job.json

# 处理任务
cat job.json

# 删除完成的任务 (job_id 为获得到的任务的 ID)
curl -X DELETE -H "X-Token: xxx" "localhost:9999/api/test/q/job/{job_id}"

# 回到前面, 继续获取任务
```

如上面的工作流, 如果有任务同时到达 q1 和 q2, 那 worker 会先获得 q1 队列中的任务.

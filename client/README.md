# Lmstfy client

## Usage

### Initialize

```
import github.com/bitleak/lmstfy/client

c := client.NewLmstfyClient(host, port, namespace, token)

c.ConfigRetry(3, 50) // optional, config the client to retry when some errors happened. retry 3 times with 50ms interval 
```

### Producer example

```
// Publish a job with ttl==forever, tries==3, delay==5s
jobID, err := c.Publish("q1", []byte("hello"), 0, 3, 5)
```

### Producer with context

Use the `*WithContext` variants to propagate cancellation or deadlines into the
HTTP request. If the context is cancelled or its deadline is exceeded, the
underlying `http.Client.Do` returns promptly and the call surfaces an `*APIError`
of type `RequestErr` — without waiting for the HTTP client's own timeout.

```
ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
defer cancel()

jobID, err := c.PublishWithContext(ctx, "q1", []byte("hello"), 0, 3, 5)
```

Same pattern applies for `PublishJobWithContext`, `RePublishWithContext`,
`RePublishJobWithContext`, and `BatchPublishWithContext`.

### Consumer example

```
// Consume a job from the q1, if there's not job available, wait until 12s passed (polling).
// And if this consumer fail to ACK the job in 10s, the job can be retried by other consumers.
job, err := c.Consume("q1", 10, 12)
if err != nil {
    panic(err)
}

// Do something with the `job`
fmt.Println(string(job.Data))

err := c.Ack("q1", job.ID)
if err != nil {
    panic(err)
}
```

```$golang
// Consume 5 jobs from the q1, if there's not job available, wait until 12s passed (polling).
// If there are some jobs but not enough 5, return jobs as much as possible.
// And if this consumer fail to ACK any job in 10s, the job can be retried by other consumers.
jobs, err := c.BatchConsume("q1", 5, 10, 12)
if err != nil {
    panic(err)
}

// Do something with the `job`
for _, job := range jobs {
    fmt.Println(string(job.Data))
    err := c.Ack("q1", job.ID)
    if err != nil {
        panic(err)
    }
}

```

> CAUTION: consume would return nil job and error when queue was empty or not found, you can enable
the client option to return error when the job is nil by revoking `EnableErrorOnNilJob()` function. 

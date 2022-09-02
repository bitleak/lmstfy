CREATE TABLE lmstfy_jobs (
    pool_name    STRING(1024),
    job_id       STRING(1024),
    namespace    STRING(1024),
    queue        STRING(1024),
    body         BYTES(MAX),
    expired_time INT64 NOT NULL,
    ready_time   INT64 NOT NULL,
    tries        INT64 NOT NULL,
    created_time  INT64 NOT NULL
) PRIMARY KEY (job_id);
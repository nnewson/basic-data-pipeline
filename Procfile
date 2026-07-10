producer: uv run producer
consumer_1: uv run consumer
consumer_2: uv run consumer
consumer_3: uv run consumer
consumer_4: uv run consumer
worker_1: RABBITMQ_QUEUE=analytics_jobs_0 uv run worker
worker_2: RABBITMQ_QUEUE=analytics_jobs_1 uv run worker
worker_3: RABBITMQ_QUEUE=analytics_jobs_2 uv run worker
worker_4: RABBITMQ_QUEUE=analytics_jobs_3 uv run worker
flink_job_submitter: uv run flink-job-submitter
flink_stats_consumer: uv run flink-stats-consumer
api: uv run api

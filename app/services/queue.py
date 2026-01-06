import redis
import time

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

QUEUE_NAME = "job_queue"
RETRY_ZSET = "job_retry_schedule"  # sorted set: score = unix timestamp, value = job_id


def enqueue_job(job_id: str):
    """Push job immediately to the main queue."""
    redis_client.lpush(QUEUE_NAME, job_id)


def schedule_retry(job_id: str, run_at_unix_ts: float):
    """
    Schedule a retry in the future using a sorted set.
    The worker will move due jobs back into QUEUE_NAME.
    """
    redis_client.zadd(RETRY_ZSET, {job_id: run_at_unix_ts})


def move_due_retries_to_queue(batch_size: int = 50):
    """
    Move any jobs whose scheduled time has passed into the main queue.
    This prevents hot-looping and enables exponential backoff retries.
    """
    now = time.time()

    # Fetch due jobs (score <= now)
    due_jobs = redis_client.zrangebyscore(RETRY_ZSET, min="-inf", max=now, start=0, num=batch_size)

    if not due_jobs:
        return 0

    # Remove from schedule and push to queue
    pipe = redis_client.pipeline()
    for job_id in due_jobs:
        pipe.zrem(RETRY_ZSET, job_id)
        pipe.lpush(QUEUE_NAME, job_id)
    pipe.execute()

    return len(due_jobs)

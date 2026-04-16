import time

from app.services.redis_client import redis_client

QUEUE_NAME = "job_queue"
RETRY_ZSET = "job_retry_schedule"  # sorted set: score = unix timestamp, value = job_id
DLQ_NAME = "job_dlq"               # list of permanently failed job IDs

# Atomic Lua script: fetch due jobs, remove from schedule, push to queue in one operation.
# Prevents double-enqueue when multiple workers call this simultaneously.
_MOVE_DUE_RETRIES_LUA = """
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #jobs == 0 then return 0 end
redis.call('ZREM', KEYS[1], unpack(jobs))
for _, job_id in ipairs(jobs) do
    redis.call('LPUSH', KEYS[2], job_id)
end
return #jobs
"""


def enqueue_job(job_id: str):
    """Push job immediately to the main queue."""
    redis_client.lpush(QUEUE_NAME, job_id)


def schedule_retry(job_id: str, run_at_unix_ts: float):
    """
    Schedule a retry in the future using a sorted set.
    The worker will move due jobs back into QUEUE_NAME.
    """
    redis_client.zadd(RETRY_ZSET, {job_id: run_at_unix_ts})


def push_to_dlq(job_id: str):
    """Move a permanently failed job into the dead-letter queue."""
    redis_client.lpush(DLQ_NAME, job_id)


def get_dlq_jobs(limit: int = 100) -> list:
    """Return up to `limit` job IDs currently in the DLQ."""
    return redis_client.lrange(DLQ_NAME, 0, limit - 1)


def remove_from_dlq(job_id: str):
    """Remove a specific job from the DLQ (called when requeuing)."""
    redis_client.lrem(DLQ_NAME, 0, job_id)


def move_due_retries_to_queue(batch_size: int = 50) -> int:
    """
    Atomically move any jobs whose scheduled time has passed into the main queue.
    Uses a Lua script to prevent double-enqueue across concurrent workers.
    """
    now = time.time()
    return redis_client.eval(
        _MOVE_DUE_RETRIES_LUA,
        2,
        RETRY_ZSET,
        QUEUE_NAME,
        now,
        batch_size,
    )

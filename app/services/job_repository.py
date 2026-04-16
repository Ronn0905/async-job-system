from typing import Dict, Optional

from app.services.redis_client import redis_client

LOCK_TTL_SECONDS = 30  # protects against stuck RUNNING if worker crashes


def _job_key(job_id: str) -> str:
    return f"job:{job_id}"


def _lock_key(job_id: str) -> str:
    return f"job_lock:{job_id}"


def create_job(job_id: str, data: Dict):
    redis_client.hset(_job_key(job_id), mapping=data)


def update_job(job_id: str, data: Dict):
    redis_client.hset(_job_key(job_id), mapping=data)


def get_job(job_id: str) -> Optional[Dict]:
    key = _job_key(job_id)
    if not redis_client.exists(key):
        return None
    return redis_client.hgetall(key)


def try_acquire_lock(job_id: str) -> bool:
    """
    Concurrency guard: if two workers accidentally get the same job_id, only one proceeds.
    SET key value NX EX <ttl> => atomic lock acquisition.
    """
    return bool(redis_client.set(_lock_key(job_id), "1", nx=True, ex=LOCK_TTL_SECONDS))


def release_lock(job_id: str):
    redis_client.delete(_lock_key(job_id))

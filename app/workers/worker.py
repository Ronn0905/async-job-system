import time
import json
import random
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()  # MUST be called before any env reads in the worker process

from app.models.job import JobStatus
from app.services.redis_client import redis_client
from app.services.job_repository import (
    get_job,
    update_job,
    try_acquire_lock,
    release_lock,
)
from app.services.queue import QUEUE_NAME, move_due_retries_to_queue, schedule_retry, push_to_dlq
from app.ai.tasks import summarize_text


def compute_backoff_seconds(retry_count: int) -> int:
    """
    Exponential backoff with jitter:
    retry 1 -> ~2s
    retry 2 -> ~4s
    retry 3 -> ~8s
    capped at 30s
    """
    base = 2 ** retry_count
    jitter = random.randint(0, 2)
    return min(base + jitter, 30)


def execute_task(task_type: str, payload: dict) -> dict:
    """Routes job execution based on task_type."""
    if task_type == "AI_TEXT_SUMMARY":
        return summarize_text(payload)

    # Fallback (legacy test)
    time.sleep(1)

    if random.random() < 0.5:
        raise RuntimeError("Simulated failure")

    return {"message": "Succeeded after possible retries"}


def process_job(job_id: str):
    """
    Executes a job with:
    - pre-lock idempotency guard
    - distributed lock
    - retries with exponential backoff
    - failure handling
    """
    # Check job state before acquiring lock — no point locking a job we won't process
    job = get_job(job_id)
    if not job:
        return
    if job.get("status") in (JobStatus.COMPLETED, JobStatus.FAILED):
        return

    if not try_acquire_lock(job_id):
        return

    try:
        # Re-fetch inside the lock to get the authoritative state
        job = get_job(job_id)
        if not job:
            return
        if job.get("status") in (JobStatus.COMPLETED, JobStatus.FAILED):
            return

        max_retries = int(job.get("max_retries", 3))
        retry_count = int(job.get("retry_count", 0))

        update_job(job_id, {
            "status": JobStatus.RUNNING,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "failure_reason": "",
        })

        try:
            task_type = job.get("task_type")
            payload = json.loads(job.get("payload") or "{}")

            result = execute_task(task_type, payload)

            update_job(job_id, {
                "status": JobStatus.COMPLETED,
                "result": json.dumps(result),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })

        except Exception as e:
            retry_count += 1

            if retry_count <= max_retries:
                backoff = compute_backoff_seconds(retry_count)
                run_at = time.time() + backoff

                update_job(job_id, {
                    "status": JobStatus.PENDING,
                    "retry_count": retry_count,
                    "failure_reason": str(e),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })

                schedule_retry(job_id, run_at)

            else:
                update_job(job_id, {
                    "status": JobStatus.FAILED,
                    "retry_count": retry_count,
                    "failure_reason": str(e),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                push_to_dlq(job_id)

    finally:
        release_lock(job_id)


def worker_loop():
    print("Worker started. Waiting for jobs...")

    while True:
        move_due_retries_to_queue()

        item = redis_client.brpop(QUEUE_NAME, timeout=2)
        if not item:
            continue

        _, job_id = item
        process_job(job_id)


if __name__ == "__main__":
    worker_loop()

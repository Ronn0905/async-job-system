import time
import json
import random
import redis
import os
from datetime import datetime

# -----------------------------
# ENV LOADING (CRITICAL)
# -----------------------------
from dotenv import load_dotenv
load_dotenv()  # MUST be called in worker process

# -----------------------------
# OpenAI
# -----------------------------
from openai import OpenAI

# -----------------------------
# Internal imports
# -----------------------------
from app.models.job import JobStatus
from app.services.job_repository import (
    get_job,
    update_job,
    try_acquire_lock,
    release_lock,
)
from app.services.queue import move_due_retries_to_queue, schedule_retry

# -----------------------------
# Redis
# -----------------------------
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

QUEUE_NAME = "job_queue"

# -----------------------------
# OpenAI Client (initialized ONCE per worker)
# -----------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY not found in environment")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

openai_client = OpenAI(api_key=OPENAI_API_KEY)

# -----------------------------
# Retry Backoff
# -----------------------------
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

# -----------------------------
# AI TASKS
# -----------------------------
def ai_text_summary(payload: dict) -> dict:
    """
    AI Text Summarization Task

    Expected payload:
    {
        "text": "long text to summarize"
    }
    """
    text = payload.get("text")
    if not text:
        raise ValueError("Missing 'text' field for AI_TEXT_SUMMARY")

    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "Summarize the following text clearly and concisely."
            },
            {
                "role": "user",
                "content": text
            }
        ],
        temperature=0.3,
    )

    summary = response.choices[0].message.content
    return {"summary": summary}

# -----------------------------
# Task Dispatcher
# -----------------------------
def execute_task(task_type: str, payload: dict) -> dict:
    """
    Routes job execution based on task_type
    """
    if task_type == "AI_TEXT_SUMMARY":
        return ai_text_summary(payload)

    # Fallback (legacy test)
    time.sleep(1)

    if random.random() < 0.5:
        raise RuntimeError("Simulated failure")

    return {"message": "Succeeded after possible retries"}

# -----------------------------
# Job Processing
# -----------------------------
def process_job(job_id: str):
    """
    Executes a job with:
    - distributed lock
    - idempotency
    - retries with exponential backoff
    - failure handling
    """
    if not try_acquire_lock(job_id):
        return

    try:
        job = get_job(job_id)
        if not job:
            return

        # Idempotency guard
        if job.get("status") == JobStatus.COMPLETED:
            return

        max_retries = int(job.get("max_retries", 3))
        retry_count = int(job.get("retry_count", 0))

        # Mark RUNNING
        update_job(job_id, {
            "status": JobStatus.RUNNING,
            "updated_at": datetime.utcnow().isoformat(),
            "failure_reason": ""
        })

        try:
            task_type = job.get("task_type")
            payload = json.loads(job.get("payload") or "{}")

            result = execute_task(task_type, payload)

            # Mark COMPLETED
            update_job(job_id, {
                "status": JobStatus.COMPLETED,
                "result": json.dumps(result),
                "updated_at": datetime.utcnow().isoformat(),
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
                    "updated_at": datetime.utcnow().isoformat(),
                })

                schedule_retry(job_id, run_at)

            else:
                update_job(job_id, {
                    "status": JobStatus.FAILED,
                    "retry_count": retry_count,
                    "failure_reason": str(e),
                    "updated_at": datetime.utcnow().isoformat(),
                })

    finally:
        release_lock(job_id)

# -----------------------------
# Worker Loop
# -----------------------------
def worker_loop():
    """
    Worker lifecycle:
    - moves due retries
    - blocks waiting for jobs
    """
    print("Worker started. Waiting for jobs...")

    while True:
        move_due_retries_to_queue()

        item = redis_client.brpop(QUEUE_NAME, timeout=2)
        if not item:
            continue

        _, job_id = item
        process_job(job_id)

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    worker_loop()

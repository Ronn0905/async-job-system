from fastapi import APIRouter, Header
from pydantic import BaseModel
from typing import Dict, Optional
from datetime import datetime
import uuid
import json

from app.models.job import JobStatus
from app.services.queue import enqueue_job
from app.services.job_repository import create_job, get_job, update_job
import redis

router = APIRouter()

# simple mapping: idempotency:{key} -> job_id
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


class JobRequest(BaseModel):
    task_type: str
    payload: Dict


@router.post("/jobs")
def create_job_api(
    request: JobRequest,
    idempotency_key: Optional[str] = Header(default=None, convert_underscores=False, alias="Idempotency-Key"),
):
    """
    Submit a new job. This endpoint NEVER executes the job.

    Idempotency basics:
    - If Idempotency-Key is provided and reused, return the same job_id.
    """
    if idempotency_key:
        existing_job_id = redis_client.get(f"idempotency:{idempotency_key}")
        if existing_job_id:
            existing = get_job(existing_job_id)
            if existing:
                return {"job_id": existing_job_id, "status": existing.get("status", JobStatus.PENDING)}

    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    job_data = {
        "status": JobStatus.PENDING,
        "task_type": request.task_type,
        "payload": json.dumps(request.payload),
        "result": "",
        "failure_reason": "",
        "retry_count": 0,
        "max_retries": 3,
        "created_at": now,
        "updated_at": now,
    }

    create_job(job_id, job_data)
    enqueue_job(job_id)

    if idempotency_key:
        # link key -> job_id so repeated submits don't create duplicate jobs
        redis_client.set(f"idempotency:{idempotency_key}", job_id, ex=60 * 60)  # 1 hour TTL

    return {"job_id": job_id, "status": JobStatus.PENDING}


@router.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return {"error": "Job not found"}

    if job.get("payload"):
        job["payload"] = json.loads(job["payload"])

    if job.get("result"):
        job["result"] = json.loads(job["result"])

    return job

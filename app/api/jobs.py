from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from datetime import datetime, timezone
import uuid
import json

from app.models.job import JobStatus
from app.services.queue import enqueue_job, get_dlq_jobs, remove_from_dlq
from app.services.job_repository import create_job, get_job, update_job
from app.services.redis_client import redis_client

router = APIRouter()


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

    If Idempotency-Key is provided and reused, return the same job_id.
    """
    if idempotency_key:
        existing_job_id = redis_client.get(f"idempotency:{idempotency_key}")
        if existing_job_id:
            existing = get_job(existing_job_id)
            if existing:
                return {"job_id": existing_job_id, "status": existing.get("status", JobStatus.PENDING)}

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

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

    # Write idempotency key BEFORE enqueuing to prevent duplicate jobs
    # from a concurrent request arriving in the window between enqueue and key write.
    if idempotency_key:
        redis_client.set(f"idempotency:{idempotency_key}", job_id, ex=60 * 60)  # 1 hour TTL

    create_job(job_id, job_data)
    enqueue_job(job_id)

    return {"job_id": job_id, "status": JobStatus.PENDING}


@router.get("/jobs/dlq")
def list_dlq():
    """List all jobs currently in the dead-letter queue."""
    job_ids = get_dlq_jobs()
    jobs = []
    for job_id in job_ids:
        job = get_job(job_id)
        if job:
            if job.get("payload"):
                job["payload"] = json.loads(job["payload"])
            if job.get("result"):
                job["result"] = json.loads(job["result"])
            jobs.append({"job_id": job_id, **job})
    return {"jobs": jobs, "count": len(jobs)}


@router.post("/jobs/{job_id}/requeue")
def requeue_job(job_id: str):
    """Reset a FAILED job and push it back to the main queue for reprocessing."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != JobStatus.FAILED:
        raise HTTPException(status_code=400, detail="Only FAILED jobs can be requeued")

    update_job(job_id, {
        "status": JobStatus.PENDING,
        "retry_count": 0,
        "failure_reason": "",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    remove_from_dlq(job_id)
    enqueue_job(job_id)
    return {"job_id": job_id, "status": JobStatus.PENDING}


@router.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("payload"):
        job["payload"] = json.loads(job["payload"])

    if job.get("result"):
        job["result"] = json.loads(job["result"])

    return job

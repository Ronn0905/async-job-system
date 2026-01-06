from typing import Dict
from app.models.job import Job

# TEMPORARY in-memory store
# (Will be replaced by DB in Phase 4)
JOB_STORE: Dict[str, Job] = {}

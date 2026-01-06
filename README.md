# Async Job Processing System

A distributed asynchronous job queue application built with FastAPI and Redis. This system allows you to submit long-running tasks via REST API and have them processed in the background by worker processes with automatic retry logic and fault tolerance.

## Features

- **Asynchronous Job Processing**: Submit jobs without waiting for completion
- **Distributed Workers**: Multiple worker processes can run in parallel
- **Fault Tolerance**: Distributed locks prevent duplicate processing
- **Automatic Retries**: Exponential backoff with up to 3 retry attempts
- **Idempotency**: Prevent duplicate job submissions with idempotency keys
- **AI Integration**: Built-in support for OpenAI-powered text summarization
- **Redis-backed**: Fast, reliable job queue and storage

## Architecture

The system consists of several key components:

- **REST API**: FastAPI endpoints for job submission and status tracking
- **Job Queue**: Redis-based FIFO queue with scheduled retry support
- **Worker Process**: Background workers that process jobs from the queue
- **Job Repository**: Redis storage for job metadata, status, and results
- **AI Tasks**: OpenAI integration for text summarization tasks

## Job Lifecycle

```
PENDING → RUNNING → COMPLETED (success)
   ↓         ↓
   ←────────┘ (retry on failure, up to 3 times)
   ↓
FAILED (after exhausting retries)
```

## Prerequisites

- Python 3.13+
- Redis server
- OpenAI API key (for AI tasks)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Ronn0905/async-job-system.git
cd async-job-system
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root directory:
```env
OPENAI_API_KEY=your_openai_api_key_here
REDIS_HOST=localhost
REDIS_PORT=6379
```

5. Start Redis server:
```bash
redis-server
```

## Usage

### Starting the API Server

```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

### Starting the Worker Process

In a separate terminal:

```bash
python -m app.workers.worker
```

You can run multiple worker processes for parallel job processing.

### API Endpoints

#### Submit a Job

```bash
POST /jobs
Content-Type: application/json
Idempotency-Key: unique-key-123 (optional)

{
  "task_type": "AI_TEXT_SUMMARY",
  "payload": {
    "text": "Your long text to summarize goes here..."
  }
}
```

Response:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PENDING"
}
```

#### Check Job Status

```bash
GET /jobs/{job_id}
```

Response:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "task_type": "AI_TEXT_SUMMARY",
  "status": "COMPLETED",
  "payload": {...},
  "result": {
    "summary": "This is the summarized text..."
  },
  "created_at": 1704531600.0,
  "updated_at": 1704531605.0,
  "retry_count": 0
}
```

### Example: Text Summarization

```python
import requests

# Submit a job
response = requests.post('http://localhost:8000/jobs', json={
    "task_type": "AI_TEXT_SUMMARY",
    "payload": {
        "text": "Long article text here..."
    }
})
job_id = response.json()['job_id']

# Check status
status_response = requests.get(f'http://localhost:8000/jobs/{job_id}')
print(status_response.json())
```

## Project Structure

```
async-job-system/
├── app/
│   ├── ai/
│   │   ├── llm_Client.py      # OpenAI client wrapper
│   │   └── tasks.py            # AI task implementations
│   ├── api/
│   │   └── jobs.py             # FastAPI endpoints
│   ├── models/
│   │   └── job.py              # Job data models
│   ├── services/
│   │   ├── job_repository.py  # Job storage and locking
│   │   ├── job_store.py        # Redis connection
│   │   └── queue.py            # Queue management
│   ├── workers/
│   │   └── worker.py           # Background worker process
│   └── main.py                 # FastAPI application
├── requirements.txt
├── .env
└── README.md
```

## Configuration

Key configuration parameters (in worker.py and job_repository.py):

- **MAX_RETRIES**: 3 (maximum retry attempts)
- **LOCK_TTL**: 30 seconds (distributed lock timeout)
- **IDEMPOTENCY_TTL**: 3600 seconds (1 hour)
- **BASE_RETRY_DELAY**: 2 seconds
- **MAX_RETRY_DELAY**: 30 seconds

## Retry Logic

The system implements exponential backoff with jitter:

- 1st retry: ~2 seconds
- 2nd retry: ~4 seconds
- 3rd retry: ~8 seconds
- Maximum delay capped at 30 seconds

## Idempotency

Prevent duplicate job submissions by including an `Idempotency-Key` header:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key" \
  -d '{"task_type": "AI_TEXT_SUMMARY", "payload": {"text": "..."}}'
```

Resubmitting with the same key returns the original job ID (valid for 1 hour).

## Distributed Processing

Multiple worker instances can run simultaneously:

- Redis distributed locks ensure each job is processed only once
- Lock TTL (30s) handles worker crashes gracefully
- Workers can be scaled horizontally for increased throughput

## Error Handling

- **Transient Failures**: Automatically retried with exponential backoff
- **Permanent Failures**: Jobs marked as FAILED after exhausting retries
- **Worker Crashes**: Distributed locks auto-release via TTL

## Development

### Running Tests

```bash
pytest
```

### API Documentation

Interactive API documentation available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

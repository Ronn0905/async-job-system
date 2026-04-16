import hashlib

from app.ai.llm_client import run_llm
from app.services.redis_client import redis_client

CACHE_TTL_SECONDS = 60 * 60 * 24  # 24 hours


def _cache_key(task_type: str, text: str) -> str:
    digest = hashlib.sha256(text.encode()).hexdigest()
    return f"cache:{task_type}:{digest}"


def summarize_text(payload: dict) -> dict:
    """
    payload must contain:
      - text: str
    returns:
      - summary: str
      - cached: bool  (True if result was served from cache)
    """
    text = payload.get("text")
    if not text:
        raise ValueError("Missing 'text' field for AI_TEXT_SUMMARY")

    key = _cache_key("AI_TEXT_SUMMARY", text)

    cached = redis_client.get(key)
    if cached:
        return {"summary": cached, "cached": True}

    prompt = f"""
You are a helpful assistant.
Summarize the following text in 5-7 bullet points.
Text:
{text}
""".strip()

    summary = run_llm(prompt)
    redis_client.set(key, summary, ex=CACHE_TTL_SECONDS)

    return {"summary": summary, "cached": False}

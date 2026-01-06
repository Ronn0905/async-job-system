from app.ai.llm_client import run_llm

def summarize_text(payload: dict) -> dict:
    """
    payload must contain:
      - text: str
    returns:
      - summary: str
    """
    text = payload.get("text", "")

    prompt = f"""
You are a helpful assistant.
Summarize the following text in 5-7 bullet points.
Text:
{text}
""".strip()

    summary = run_llm(prompt)

    return {"summary": summary}

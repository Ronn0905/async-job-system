import os
from openai import OpenAI

# Create one client instance (cheap + clean)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def run_llm(prompt: str) -> str:
    """
    Calls the LLM and returns plain text output.
    Kept small on purpose.
    """
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.2,
    )

    return resp.choices[0].message.content.strip()

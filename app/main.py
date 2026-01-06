from fastapi import FastAPI
from dotenv import load_dotenv

from app.api import jobs

# Load environment variables from .env
load_dotenv()

app = FastAPI(title="Async Job Processing System")

app.include_router(jobs.router)

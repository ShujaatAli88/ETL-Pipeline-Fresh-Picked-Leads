import os
from dotenv import load_dotenv

load_dotenv(override=True)

class Config:
    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET_ID = os.getenv("DATASET_ID")
    PASSWORD = os.getenv("PASSWORD")
    EMAIL = os.getenv("EMAIL")
    SENTRY_DSN = os.getenv("SENTRY_DSN")
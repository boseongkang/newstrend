from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseModel):
    newsapi_key: str | None = os.getenv("NEWSAPI_KEY")
    default_country: str = os.getenv("COUNTRY", "us")

settings = Settings()

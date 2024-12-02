import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

DATABASE_URL = os.getenv("DATABASE_URL")
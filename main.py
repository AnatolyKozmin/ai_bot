import asyncio
from dotenv import load_dotenv

from collector import run

# Load environment variables from .env
load_dotenv()


if __name__ == "__main__":
    asyncio.run(run())

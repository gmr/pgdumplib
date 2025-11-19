from dotenv import load_dotenv


def setup_module():
    """Load environment variables from .env file"""
    # Load from .env file if it exists
    load_dotenv()

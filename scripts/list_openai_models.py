import os
import openai
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("Error: OPENAI_API_KEY not found in .env")
    exit(1)

client = openai.OpenAI(api_key=api_key)

try:
    models = client.models.list()
    print("Available OpenAI Models:")
    # Sort and filter for common models to keep output manageable
    model_ids = sorted([m.id for m in models.data])
    for m_id in model_ids:
        print(f" - {m_id}")
except Exception as e:
    print(f"Error listing models: {e}")

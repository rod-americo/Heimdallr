import base64
import requests
import json
import os

# Create a dummy white image (100x100)
from PIL import Image
import io

def create_test_image():
    img = Image.new('RGB', (100, 100), color='white')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def test_api():
    print("Generating test image...")
    b64_img = create_test_image()
    
    payload = {
        "image": b64_img,
        "prompt": "Is this a normal chest X-ray?"
    }
    
    url = "http://localhost:8001/api/ap-thorax-xray"
    print(f"Sending request to {url}...")
    
    try:
        response = requests.post(url, json=payload, timeout=200)
        
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("Response:")
            print(json.dumps(response.json(), indent=2))
            print("\nSUCCESS: End-to-end test passed!")
        else:
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"Failed to connect: {e}")
        print("Ensure both 'server.py' (port 8001) and 'medgemma_api.py' (port 8002) are running.")

if __name__ == "__main__":
    test_api()

import requests
import json
import os
from PIL import Image
import io

def create_test_image():
    # Create a dummy image
    img = Image.new('RGB', (100, 100), color='white')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()

def test_api():
    print("Generating test image (PNG)...")
    img_data = create_test_image()
    
    url = "http://localhost:8001/api/ap-thorax-xray"
    print(f"Sending Multipart request to {url}...")
    
    try:
        # Multipart Upload
        files = {
            'file': ('test.png', img_data, 'image/png')
        }
        data = {
            'prompt': "Translate findings to Portuguese",
            'age': "55-year-old"
        }
        
        response = requests.post(url, files=files, data=data, timeout=200)
        
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("Response:")
            print(json.dumps(response.json(), indent=2))
            print("\nSUCCESS: End-to-end Multipart test passed (with Age)!")
        else:
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"Failed to connect: {e}")
        print("Ensure both 'server.py' (port 8001) and 'medgemma_api.py' (port 8002) are running.")

if __name__ == "__main__":
    test_api()

import httpx
import time
import sys
import os

def test_anthropic_api():
    url = "http://localhost:8003/analyze"
    # File path found in previous step
    file_path = "data/dataset/5481121_2154578/1.3.46.670589.30.36.0.1.18774231851.1767534719666.2.dcm"
    
    if not os.path.exists(file_path):
        print(f"Error: Test file not found at {file_path}")
        return

    print(f"Testing Anthropic API at {url} with file {file_path}")
    
    # Multipart form data
    files = {'file': open(file_path, 'rb')}
    data = {
        'age': '45',
        'identificador': 'test_case_001'
    }

    try:
        response = httpx.post(url, files=files, data=data, timeout=120.0)
        
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("Response JSON:")
            print(response.json())
            print("\nSUCCESS: API returned 200 OK")
        else:
            print("Response Text:")
            print(response.text)
            print("\nFAILURE: API returned error")

    except Exception as e:
        print(f"Exception during request: {e}")

if __name__ == "__main__":
    # Wait for service to start
    print("Waiting 5s for service to start...")
    time.sleep(5)
    test_anthropic_api()

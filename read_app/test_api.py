import requests
import threading

url = "http://127.0.0.1:8000/api/readOperations/"
file_path = "/mnt/c/Users/Public/Mahi/ray_django_project/read_file/color_srgb.csv"

payload = {
    "file_path": file_path
}

headers = {
    "Content-Type": "application/json"
}

def call_api(i):
    response = requests.post(url, json=payload, headers=headers)
    print(f"Thread {i} - Status Code:", response.status_code)
    try:
        print(f"Thread {i} - Response JSON:", response.json())
    except requests.exceptions.JSONDecodeError:
        print(f"Thread {i} - Response Text:", response.text)

threads = []
for i in range(2):  
    t = threading.Thread(target=call_api, args=(i,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

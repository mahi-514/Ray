import requests

abort_url = "http://127.0.0.1:8000/api/abortExecution/"  
ray_id = "946e938fe88c43d9993e03063d1cdf6e"  

payload = {
    "ray_id": ray_id 
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(abort_url, json=payload, headers=headers)

print("Status Code:", response.status_code)
try:
    print("Response:", response.json())
except requests.exceptions.JSONDecodeError:
    print("Response Text:", response.text)

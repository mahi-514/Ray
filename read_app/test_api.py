import requests

url = "http://127.0.0.1:8000/api/readOperations/"
file_path = "/mnt/c/Users/Public/Mahi/ray_django_project/read_file/color_srgb.csv"

payload = {
    "file_path": file_path
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

print("Status Code:", response.status_code)
try:
    print("Response JSON:", response.json())
except requests.exceptions.JSONDecodeError:
    print("Response Text:", response.text)

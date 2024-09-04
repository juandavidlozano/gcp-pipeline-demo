import requests
import json

# URL of the API
url = 'https://jsonplaceholder.typicode.com/posts'

# Fetch data from API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

    # Save the API response to a file
    with open('api_response.json', 'w') as f:
        json.dump(data, f, indent=4)

    print("API response has been saved to api_response.json")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

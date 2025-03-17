import requests

def test_redis_api():
    # URL for your endpoint (adjust host/port as needed)
    url = "http://localhost:5005/api/redis/total_pokemons_hourly"

    # Replace with your actual secret values from your configuration:
    # For example, if your config has:
    #   api_header_name = "X-API-Secret"
    #   api_header_secret = "MY_HEADER_SECRET"
    #   api_secret_key = "MY_SECRET_KEY"
    secret_header_name = "X-API-Secret"   # Must match your AppConfig.api_header_name
    secret_header_value = "MY_HEADER_SECRET"  # Replace with your actual header secret
    secret_key_value = "MY_SECRET_KEY"  # Replace with your actual secret key

    # Setup header and query parameters
    headers = {
        secret_header_name: secret_header_value
    }
    params = {
        "api_secret_key": secret_key_value
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        print("Status Code:", response.status_code)
        print("Response JSON:", response.json())
    except Exception as e:
        print("Error making request:", e)

if __name__ == "__main__":
    test_redis_api()

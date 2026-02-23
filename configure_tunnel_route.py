import requests
import json
import sys

# Cloudflare API configuration
ACCOUNT_ID = "b518bf789d6efa3f1c374bb385df9882"  # From the token
TUNNEL_ID = "81e73a38-7bab-4838-8802-a358d34ae8ae"

# Get API token from command line argument
if len(sys.argv) > 1:
    api_token = sys.argv[1]
else:
    print("Usage: python configure_tunnel_route.py YOUR_API_TOKEN")
    print("\nTo get your API token:")
    print("1. Go to https://dash.cloudflare.com/profile/api-tokens")
    print("2. Create a token with 'Cloudflare Tunnel' Edit permissions")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

# Configure tunnel with ingress rules
url = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/cfd_tunnel/{TUNNEL_ID}/configurations"

config = {
    "config": {
        "ingress": [
            {
                "hostname": "callansweringandy.uk",
                "service": "http://127.0.0.1:5004"
            },
            {
                "hostname": "www.callansweringandy.uk",
                "service": "http://127.0.0.1:5004"
            },
            {
                "hostname": "payasyoumow.org",
                "service": "http://127.0.0.1:5015"
            },
            {
                "hostname": "www.payasyoumow.org",
                "service": "http://127.0.0.1:5015"
            },
            {
                "service": "http_status:404"
            }
        ],
        "warp-routing": {
            "enabled": False
        }
    }
}

print("Configuring tunnel routes...")
response = requests.put(url, headers=headers, json=config)

if response.status_code in [200, 201]:
    print("\n✅ SUCCESS! Tunnel route configured successfully!")
    print("\nIngress rules set:")
    print("  - callansweringandy.uk → http://127.0.0.1:5004")
    print("  - payasyoumow.org → http://127.0.0.1:5015")
    print("\nNext steps:")
    print("1. Restart your tunnel using the Booking App Manager")
    print("2. Visit https://payasyoumow.org")
else:
    print(f"\n❌ Error: {response.status_code}")
    print(response.text)

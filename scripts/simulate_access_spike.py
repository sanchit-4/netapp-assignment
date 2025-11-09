import requests
import sys
import time

def simulate_access_spike(object_id, count):
    """
    Calls the access API endpoint for a given object multiple times.
    """
    print(f"Simulating {count} accesses for object {object_id}...")
    base_url = "http://localhost:8000"
    for i in range(count):
        try:
            response = requests.post(f"{base_url}/api/objects/{object_id}/access")
            response.raise_for_status()
            print(f"Access {i+1}/{count}", end='\r')
            time.sleep(0.05) # Small delay to avoid overwhelming the server
        except requests.exceptions.RequestException as e:
            print(f"\nError accessing API: {e}")
            break
    print(f"\nFinished simulating {count} accesses.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/simulate_access_spike.py <object_id> [access_count]")
        sys.exit(1)
    
    object_id = sys.argv[1]
    # Access the object 101 times to cross the hot tier threshold, or use the provided count
    access_count = int(sys.argv[2]) if len(sys.argv) > 2 else 101
    simulate_access_spike(object_id, access_count)
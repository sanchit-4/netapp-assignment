import requests
import json
import sys
import time
import argparse

API_BASE_URL = "http://localhost:8000"

def call_api(method, endpoint, data=None):
    try:
        url = f"{API_BASE_URL}{endpoint}"
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=data)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling API {endpoint}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        sys.exit(1)

def train_model_cli():
    print("Initiating ML model training...")
    result = call_api("POST", "/api/ml/train")
    print(json.dumps(result, indent=2))

def ingest_object_cli(file_path=None):
    # This re-uses the existing script logic for simplicity
    # In a real CLI, you might want to integrate it directly
    import subprocess
    print("Ingesting new object...")
    try:
        command = [sys.executable, "scripts/produce_test_message.py"]
        if file_path:
            command.extend(["--file", file_path])

        print(f"Executing command: {' '.join(command)}")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        # Extract object_id from output
        if "object_id:" in result.stdout:
            object_id = result.stdout.split("object_id:")[1].strip().split('\n')[0]
            print(f"Ingested object ID: {object_id}")
            return object_id
    except subprocess.CalledProcessError as e:
        print(f"Error ingesting object: {e.stderr}")
        sys.exit(1)

def simulate_access_cli(object_id, count=1):
    # This re-uses the existing script logic for simplicity
    import subprocess
    print(f"Simulating {count} accesses for object {object_id}...")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/simulate_access_spike.py", object_id, str(count)],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error simulating access: {e.stderr}")
        sys.exit(1)

def get_objects_cli():
    print("Fetching all objects...")
    objects = call_api("GET", "/api/objects")
    print(json.dumps(objects, indent=2))

def get_metrics_cli():
    print("Fetching system metrics...")
    metrics = call_api("GET", "/api/metrics")
    print(json.dumps(metrics, indent=2))

def get_migrations_cli():
    print("Fetching recent migration events...")
    migrations = call_api("GET", "/api/migrations")
    print(json.dumps(migrations, indent=2))

def predict_access_cli(object_id):
    print(f"Predicting access for object {object_id}...")
    prediction = call_api("GET", f"/api/objects/{object_id}/predict")
    print(json.dumps(prediction, indent=2))

def main():
    parser = argparse.ArgumentParser(description="CLI client for Data in Motion API.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Train model command
    train_parser = subparsers.add_parser("train-model", help="Train the ML model.")
    train_parser.set_defaults(func=lambda args: train_model_cli())

    # Ingest object command
    ingest_parser = subparsers.add_parser("ingest-object", help="Ingest a new object.")
    ingest_parser.add_argument("--file", help="Path to a file whose content will be ingested.")
    ingest_parser.set_defaults(func=lambda args: ingest_object_cli(args.file))

    # Simulate access command
    access_parser = subparsers.add_parser("simulate-access", help="Simulate access to an object.")
    access_parser.add_argument("object_id", help="ID of the object to access.")
    access_parser.add_argument("--count", type=int, default=1, help="Number of times to simulate access.")
    access_parser.set_defaults(func=lambda args: simulate_access_cli(args.object_id, args.count))

    # Get objects command
    objects_parser = subparsers.add_parser("get-objects", help="Get all objects.")
    objects_parser.set_defaults(func=lambda args: get_objects_cli())

    # Get metrics command
    metrics_parser = subparsers.add_parser("get-metrics", help="Get system metrics.")
    metrics_parser.set_defaults(func=lambda args: get_metrics_cli())

    # Get migrations command
    migrations_parser = subparsers.add_parser("get-migrations", help="Get recent migration events.")
    migrations_parser.set_defaults(func=lambda args: get_migrations_cli())

    # Predict access command
    predict_parser = subparsers.add_parser("predict-access", help="Predict access for an object.")
    predict_parser.add_argument("object_id", help="ID of the object for prediction.")
    predict_parser.set_defaults(func=lambda args: predict_access_cli(args.object_id))

    args = parser.parse_args()

    if args.command:
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

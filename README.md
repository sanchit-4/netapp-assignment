# Data Migration System

This project is a simulation of an intelligent data management system that automatically manages the storage tier of objects based on their usage patterns. It uses both reactive and proactive strategies to optimize data placement, ensuring a balance between cost and performance.

## Core Concepts

The system employs two distinct mechanisms for data migration:

### 1. Reactive Migration

This mechanism responds to real-time access patterns. When an object experiences a sudden and significant increase in access requests (a "spike"), the system identifies it as "hot" and automatically migrates it to a higher-performance storage tier. This ensures that popular data is served from the fastest storage available.

- **Trigger:** A high number of recent accesses.
- **Advantage:** Responds quickly to immediate changes in demand.

### 2. Proactive Migration

This is the more advanced feature of the system. It uses a machine learning model to forecast the future popularity of an object. Based on this prediction, the system can move an object to a faster tier *before* the demand actually occurs.

- **Trigger:** A high predicted access count from the ML model.
- **Advantage:** Improves user experience by anticipating needs and eliminating the initial period of slow access that occurs in a purely reactive system.

## Setup Instructions

Follow these steps to get the project running.

### Step 1: Environment Variables

The system requires environment variables for configuration, including credentials for cloud storage.

1.  Create a `.env` file by copying the example file:
    ```bash
    cp .env.example .env
    ```
2.  **Edit the `.env` file:** You must fill in your AWS S3 credentials. The MinIO and Kafka variables are pre-configured for Docker.
    ```dotenv
    # .env
    # ... (other variables)

    # AWS S3 Configuration - IMPORTANT: Fill these in with your credentials
    AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
    AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY
    AWS_S3_BUCKET_NAME=your-s3-bucket-name
    AWS_REGION=your-s3-bucket-region

    # ... (other variables)
    ```

### Step 2: Start the Services

The entire application stack, including the backend service, Kafka, MinIO, and Zookeeper, is managed by Docker Compose.

```bash
docker-compose up -d
```
If you make changes to the backend code and need to rebuild the image, use:
```bash
docker-compose up --build -d backend
```

### Step 3: Create Kafka Topics

The backend service requires specific Kafka topics to be created. Run the following command to create them:

```bash
docker-compose exec backend python create_kafka_topics.py
```

### Step 4: Train the Initial ML Model

The system uses a machine learning model for proactive migrations. 

```

## Demonstration Walkthrough

Throughout this walkthrough, it's highly recommended to monitor the backend logs in a separate terminal to observe the system's real-time behavior:

```bash
docker-compose logs -f backend
```

This is a step-by-step guide to demonstrate the core features of the system.

### 1. Ingest a New Object

Start by ingesting a new file. It will be placed in the default `public` tier. Note the `object_id` that is returned.

```bash
python scripts/cli_client.py ingest-object --file sample_document.txt
```

### 2. Demonstrate Reactive Migration

Next, simulate a heavy access load on the object to trigger a reactive migration.

```bash
# Replace <your-object-id> with the ID from the previous step
python scripts/cli_client.py simulate-access <your-object-id> --count 100
```

After a few moments, check the migration history. You should see a `completed` migration with the reason `reactive_access_spike`.

```bash
python scripts/cli_client.py get-migrations
```

Verify the object's new location. It should have moved from `public` to `private` or `onprem`.

```bash
python scripts/cli_client.py get-objects
```

### 3. Demonstrate Proactive Migration

The proactive service runs every minute. Ingest a new, different file:

```bash
python scripts/cli_client.py ingest-object --file another_sample.txt
```

Now, simply wait for about 65 seconds. The proactive service will analyze the new object, and the ML model will predict its future usage. If the prediction is high enough, it will trigger a migration.

After waiting, check the migration history. You should see a new migration for this object with the reason `proactive_prediction`.

```bash
python scripts/cli_client.py get-migrations
```

## CLI Client Usage

A CLI client is provided in the `scripts` directory to interact with the system.

| Command             | Description                                     |
| ------------------- | ----------------------------------------------- |
| `train-model`       | Trains the machine learning model.              |
| `ingest-object`     | Ingests a new object into the system.           |
| `simulate-access`   | Simulates access to an object.                  |
| `get-objects`       | Retrieves a list of all objects in the system.  |
| `get-metrics`       | Fetches system-wide metrics.                    |
| `get-migrations`    | Fetches the history of migration events.        |
| `predict-access`    | Predicts future access for a specific object.   |

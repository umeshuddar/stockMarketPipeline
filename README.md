# Stock Market Data Pipeline

An end-to-end stock market data engineering pipeline built with **Kafka, MinIO, Apache Spark, Airflow, Docker, and Snowflake**.

The pipeline ingests stock market data through Kafka, stores the raw data in MinIO, processes it with Apache Spark, and loads the resulting datasets into Snowflake for analytics.

---

## Architecture

```text
                    Stock Market Data
                           │
                           ▼
                    ┌─────────────┐
                    │    Kafka    │
                    │   Streaming │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │    MinIO    │
                    │ Raw Storage │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ Apache Spark│
                    │ Processing  │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   MinIO     │
                    │  Parquet    │
                    │  Processed  │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  Snowflake  │
                    │  Analytics  │
                    └─────────────┘

                 Airflow orchestrates
                   the entire pipeline
```

---

## Pipeline

The pipeline consists of the following stages:

1. **Stock Data Production** – Stock market data is produced and published to Kafka.
2. **Kafka Streaming** – Kafka acts as the messaging layer for stock data.
3. **MinIO Storage** – Kafka data is consumed and stored as Parquet files in MinIO.
4. **Spark Processing** – Apache Spark reads the raw data, performs transformations, calculates price changes, and generates summary statistics.
5. **Processed Parquet** – Spark writes detailed and aggregated datasets back to MinIO.
6. **Snowflake Loading** – Processed datasets are loaded into Snowflake for analytical workloads.
7. **Airflow Orchestration** – Airflow manages and executes the pipeline tasks in the correct order.

---

## Components

| Component        | Purpose                                                                   |
| ---------------- | ------------------------------------------------------------------------- |
| **Kafka**        | Streams stock market data between pipeline components.                    |
| **MinIO**        | S3-compatible object storage for raw and processed Parquet files.         |
| **Apache Spark** | Processes stock data and calculates price changes and summary statistics. |
| **Airflow**      | Orchestrates and schedules the data pipeline.                             |
| **Snowflake**    | Stores processed stock data for analytics and reporting.                  |
| **Docker**       | Runs the pipeline services in local containers.                           |

---

## Airflow DAG

### DAG Name

```text
stock_market_batch_pipeline
```

### Task Flow

```text
produce_stock_data
        ↓
consume_to_minio
        ↓
spark_batch_process
        ↓
check_minio_output
        ↓
load_to_snowflake
```

### Tasks

#### `produce_stock_data`

Produces stock market data and publishes it to Kafka.

#### `consume_to_minio`

Consumes stock data from Kafka and stores the raw data in MinIO as Parquet files.

#### `spark_batch_process`

Runs the Spark batch processing job.

The processing stage:

* Reads raw stock data from MinIO.
* Processes and transforms the records.
* Calculates stock price changes.
* Generates summary statistics.
* Writes processed data back to MinIO.

#### `check_minio_output`

Validates that the expected Spark output has been successfully written to MinIO before continuing.

#### `load_to_snowflake`

Loads the processed stock datasets from MinIO into Snowflake for analytics.

---

## Project Structure

```text
StockMarketPipeline/
│
├── src/
│   ├── airflow/
│   │   └── dags/
│   │       └── stock_market_batch_dag.py
│   │
│   ├── processing/
│   │   └── spark_batch_processor.py
│   │
│   └── snowflake/
│       └── load_to_snowflake.py
│
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

---

## MinIO Storage

MinIO is used as the object storage layer for both raw and processed stock market data.

### Raw / Realtime Data

Raw Parquet files are stored under:

```text
realtime-stock-data/realtime/
```

### Processed Data

Processed data is organized by processing date:

```text
stock-market-data/processed/YYYY-MM-DD/
├── detailed/
└── summary/
```

### Detailed Dataset

The `detailed` directory contains the processed stock records.

These records contain the transformed stock market data used for downstream analytics.

### Summary Dataset

The `summary` directory contains aggregated statistics for each ticker.

This dataset can be used for reporting and analytical queries.

---

## Snowflake

Snowflake is used as the final analytical data warehouse.

Processed stock market data is loaded from MinIO into Snowflake after the Spark processing stage has completed successfully.

### Environment Variables

Snowflake credentials and connection settings are stored in the `.env` file:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STOCK_MARKET_DB
SNOWFLAKE_SCHEMA=PUBLIC
```

> **Important:** Never commit `.env` to Git.

Make sure `.env` is included in `.gitignore`:

```gitignore
.env
```

---

## Prerequisites

Before running the project, make sure you have:

* Docker
* Docker Compose
* Git

Verify your Docker installation:

```bash
docker --version
docker compose version
```

---

## Running the Project

### 1. Clone the Repository

```bash
git clone <repository-url>
cd StockMarketPipeline
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STOCK_MARKET_DB
SNOWFLAKE_SCHEMA=PUBLIC
```

Update the values with your Snowflake credentials.

### 3. Start the Services

Start all services using Docker Compose:

```bash
docker compose up -d
```

### 4. Check Running Services

```bash
docker compose ps
```

All required containers should be running before triggering the Airflow DAG.

### 5. Stop the Services

To stop the project:

```bash
docker compose down
```

To stop the services and remove associated volumes:

```bash
docker compose down -v
```

> Use `-v` carefully because it removes Docker volumes and may delete locally stored service data.

---

## Service URLs

Once the containers are running, the following services are available locally:

| Service             | URL                   |
| ------------------- | --------------------- |
| **Airflow**         | http://localhost:8081 |
| **MinIO Console**   | http://localhost:9001 |
| **Kafka UI**        | http://localhost:8082 |
| **Spark Master UI** | http://localhost:9090 |

### Airflow

Open the Airflow web interface:

```text
http://localhost:8081
```

Find the DAG:

```text
stock_market_batch_pipeline
```

Enable the DAG and trigger it manually.

### MinIO

Open the MinIO console:

```text
http://localhost:9001
```

Use the console to inspect the raw and processed Parquet files.

### Kafka UI

Open the Kafka UI:

```text
http://localhost:8082
```

Use it to inspect Kafka topics, messages, consumers, and brokers.

### Spark

Open the Spark Master UI:

```text
http://localhost:9090
```

Use it to monitor Spark workers and running applications.

---

## End-to-End Workflow

Once the Airflow DAG is triggered, the pipeline executes the following workflow:

```text
1. Produce stock data
          │
          ▼
2. Publish data to Kafka
          │
          ▼
3. Consume Kafka messages
          │
          ▼
4. Store raw data in MinIO
          │
          ▼
5. Run Spark batch processing
          │
          ▼
6. Generate detailed dataset
          │
          ▼
7. Generate summary dataset
          │
          ▼
8. Store processed Parquet in MinIO
          │
          ▼
9. Validate MinIO output
          │
          ▼
10. Load processed data into Snowflake
```

---

## Data Flow

```text
Stock Data
    │
    ▼
 Kafka Topic
    │
    ▼
 MinIO
 └── realtime-stock-data/
     └── realtime/
          │
          ▼
     Apache Spark
          │
          ├───────────────┐
          ▼               ▼
     detailed/         summary/
          │               │
          └───────┬───────┘
                  ▼
               MinIO
                  │
                  ▼
              Snowflake
                  │
                  ▼
              Analytics
```

---

## Technologies

* **Python** – Pipeline and data-processing code
* **Apache Kafka** – Event streaming
* **MinIO** – Object storage
* **Apache Spark** – Batch data processing
* **Apache Airflow** – Workflow orchestration
* **Snowflake** – Cloud data warehouse
* **Docker** – Containerized development environment
* **Parquet** – Columnar data storage format

---

## Development Notes

The project is designed to run locally using Docker Compose.

The pipeline separates:

* Data ingestion
* Object storage
* Data processing
* Workflow orchestration
* Data warehousing

This architecture makes it easier to develop, test, monitor, and extend individual components independently.

---

## Security

Do not commit sensitive credentials to the repository.

At minimum, add the following to `.gitignore`:

```gitignore
.env
__pycache__/
*.pyc
.idea/
.vscode/
```

For production environments, use a dedicated secrets-management solution instead of storing credentials directly in environment files.

---

## Future Improvements

Potential improvements include:

* Add real-time Spark Structured Streaming.
* Add data quality checks.
* Add schema validation.
* Add retry and failure handling in Airflow.
* Add automated tests.
* Add monitoring and alerting.
* Add incremental Snowflake loading.
* Add partitioning and optimization for large datasets.
* Add dashboards for stock market analytics.
* Deploy the pipeline to a cloud environment.

---

## License

Add your preferred license here.

For example:

```text
MIT License
```

---

## Author

**Stock Market Pipeline**

Built as an end-to-end data engineering project demonstrating Kafka, MinIO, Spark, Airflow, Docker, Parquet, and Snowflake integration.

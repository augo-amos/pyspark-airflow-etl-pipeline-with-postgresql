# PySpark-Airflow Data Pipeline Project

A complete data engineering pipeline that processes customer transaction data using PySpark, orchestrates workflows with Apache Airflow, and stores results in PostgreSQL - all running in Docker containers.

## Project Overview

This project demonstrates a real-world ETL (Extract, Transform, Load) pipeline that:
- **Extracts** customer transaction data from CSV files
- **Transforms** data using PySpark for aggregation and analysis
- **Loads** processed results into PostgreSQL database
- **Orchestrates** the entire workflow using Apache Airflow
- **Containers** all components using Docker for easy deployment

## Project Structure

```
pyspark-airflow-project/
├── docker-compose.yml          # Main Docker composition
├── airflow/
│   ├── dags/
│   │   └── customer_pipeline.py # Airflow DAG definition
│   └── requirements.txt        # Python dependencies
├── spark/
│   └── scripts/
│       └── process_data.py     # PySpark data processing script
├── data/
│   └── customers.csv           # Sample customer transaction data
├── postgres/
│   └── init.sql               # Database schema initialization
└── scripts/
│    └── read_postgres.py       # Utility script to read PostgreSQL data
├── images/
    └── airflow.png
    └── postgres_tables.png
```

## Tech Stack

- **Apache Airflow 2.8.1** - Workflow orchestration
- **PySpark 3.5.0** - Distributed data processing
- **PostgreSQL 13** - Data storage
- **Docker & Docker Compose** - Containerization
- **Python 3.8+** - Scripting and data processing

## Quick Start

### Prerequisites
- Docker
- Docker Compose

### 1. Clone and Setup
```bash
git clone <repository-url>
cd pyspark-airflow-project
```

### 2. Start the Pipeline
```bash
# Build and start all services
docker compose up -d --build

# Monitor initialization
docker compose logs -f airflow-init
```

### 3. Access the Services

- **Airflow UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **Spark Master UI**: http://localhost:8081
- **PostgreSQL**: `localhost:5433`

### 4. Run the Pipeline

1. Open Airflow UI at http://localhost:8080
2. Find the `customer_data_pipeline` DAG
3. Toggle the switch to activate it (OFF → ON)
4. Click the "Play" button to trigger execution
5. Monitor the progress in the Airflow UI

## Data Pipeline Flow

### Input Data (`data/customers.csv`)
Sample customer transaction data with:
- Customer demographics (age, gender, marital status, income level)
- Transaction details (date, amount, product category)

### Processing Steps
1. **Extract**: Read CSV data using PySpark
2. **Transform**:
   - Aggregate spending by customer
   - Calculate transaction statistics
   - Group by product categories
3. **Load**: Store results in PostgreSQL tables:
   - `customer_summary` - Customer-level aggregations
   - `product_category_stats` - Product category insights

### Output Tables

**customer_summary:**
- `customer_id`, `total_spent`, `transaction_count`, `avg_transaction`, `last_transaction_date`

**product_category_stats:**
- `product_category`, `total_revenue`, `transaction_count`, `avg_transaction_amount`

## Querying the Data

### Method 1: Direct PostgreSQL Access
```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U airflow -d customer_analytics

# View customer data
SELECT * FROM customer_summary;

# View product statistics  
SELECT * FROM product_category_stats;

# Exit
\q
```

### Method 2: One-Line Queries
```bash
# Quick data verification
docker compose exec postgres psql -U airflow -d customer_analytics -c "SELECT * FROM customer_summary;"
docker compose exec postgres psql -U airflow -d customer_analytics -c "SELECT * FROM product_category_stats;"
```

### Method 3: Using Python Script
```bash
python scripts/read_postgres.py
```

## Container Services

| Service | Container Name | Port | Purpose |
|---------|----------------|------|---------|
| PostgreSQL | `pyspark-airflow-project-postgres-1` | 5433 | Data storage |
| Airflow Webserver | `pyspark-airflow-project-airflow-webserver-1` | 8080 | Web UI & API |
| Airflow Scheduler | `pyspark-airflow-project-airflow-scheduler-1` | - | Task scheduling |
| Spark Master | `pyspark-airflow-project-spark-master-1` | 8081 | Spark cluster management |
| Spark Worker | `pyspark-airflow-project-spark-worker-1` | - | Distributed data processing |

## Configuration

### Database Connections
- **PostgreSQL**: `postgresql://airflow:airflow@postgres:5432/airflow` (internal)
- **Customer Analytics DB**: `postgresql://airflow:airflow@localhost:5433/customer_analytics` (external)

### Airflow Configuration
- Executor: `LocalExecutor`
- Database: PostgreSQL
- Fernet Key: Pre-configured for security
- DAGs Folder: `/opt/airflow/dags`

## Sample Analytics Queries

After pipeline execution, try these analytical queries:

```sql
-- Top spending customers
SELECT * FROM customer_summary ORDER BY total_spent DESC LIMIT 5;

-- Revenue by product category
SELECT product_category, total_revenue 
FROM product_category_stats 
ORDER BY total_revenue DESC;

-- Customer demographics analysis
SELECT 
    gender,
    marital_status,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_spending
FROM customer_summary 
GROUP BY gender, marital_status;
```

## Development

### Adding New DAGs
1. Place Python files in `airflow/dags/`
2. Airflow automatically detects and loads them
3. Access via Airflow UI at http://localhost:8080

### Modifying Data Processing
1. Update `spark/scripts/process_data.py`
2. Changes take effect on next DAG run

### Adding Dependencies
1. Update `airflow/requirements.txt`
2. Rebuild containers: `docker compose up -d --build`

## Managing the Pipeline

### Start/Stop Services
```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# Stop and remove volumes (cleans data)
docker compose down -v

# View logs
docker compose logs -f
docker compose logs -f airflow-webserver
docker compose logs -f postgres
```

### Monitoring
- **Airflow UI**: DAG status, task logs, execution history
- **Spark UI**: Cluster status, job progress
- **Container logs**: Real-time debugging

## Troubleshooting

### Common Issues

**Port conflicts**: 
- Change ports in `docker-compose.yml`
- PostgreSQL default: 5433 (host) → 5432 (container)

**DAG not appearing**:
- Check DAG file syntax
- Restart Airflow: `docker compose restart airflow-webserver airflow-scheduler`

**Database connection issues**:
- Verify PostgreSQL is healthy: `docker compose ps`
- Check logs: `docker compose logs postgres`

### Useful Commands
```bash
# Check container status
docker compose ps

# Test PySpark installation
docker compose exec airflow-webserver python -c "import pyspark; print('PySpark OK')"

# Test database connection
docker compose exec postgres psql -U airflow -d airflow -c "SELECT version();"

# View Airflow DAG list
docker compose exec airflow-webserver airflow dags list
```

## Key Features

- **Complete ETL Pipeline** - End-to-end data processing
- **Containerized** - Easy deployment and isolation
- **Orchestrated** - Automated workflow management
- **Scalable** - Spark-based distributed processing
- **Monitoring** - Web UIs for all components
- **Data Persistence** - PostgreSQL with volume storage
- **Error Handling** - Robust task failure management

## Use Cases

This project demonstrates patterns for:
- Customer analytics pipelines
- ETL workflow orchestration
- Docker-based data engineering
- PySpark data processing
- Airflow DAG development
- PostgreSQL data storage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test with `docker compose up -d --build`
4. Submit a pull request


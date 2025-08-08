# Deel Product Analytics Challenge

A comprehensive data analytics solution for Globepay payment processing data, featuring exploratory data analysis, SQL modeling, and dbt data transformations.

## 📋 Project Overview

This project analyzes Globepay transaction data to provide insights into:
- **Acceptance Rates**: Transaction approval patterns by country and time
- **Declined Volume Analysis**: High-value declined transactions by geography
- **Chargeback Impact**: Potential revenue loss from chargebacks
- **Business Intelligence**: Interactive dashboards and reporting models

## 🏗️ Project Structure

```
deel/
├── input_data/                          # Raw CSV data files
│   ├── acceptance_report.csv            # Transaction acceptance data
│   └── chargeback_report.csv           # Chargeback data
├── eda_deel.ipynb                       # Exploratory Data Analysis notebook
├── deel_dbt/                           # dbt data modeling project
│   ├── models/
│   │   ├── sources/                     # Source definitions
│   │   ├── staging/                     # Data cleaning & standardization
│   │   ├── intermediate/                # Business logic & enrichment
│   │   └── curated/                     # Analytics-ready tables
│   │       ├── finance/                 # Financial metrics models
│   │       ├── performance/             # Performance KPI models
│   │       └── analytics/               # Advanced analytics models
│   ├── seeds/                          # Reference data (calendar)
│   ├── tests/                          # Data quality tests
│   └── dbt_project.yml                # dbt configuration
├── requirements.txt                    # Python dependencies
├── README.md                          # This file
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL database (for dbt models)
- Git

### 1. Environment Setup

#### Clone and Navigate
```bash
git clone <repository-url>
cd deel
```

#### Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
```

#### Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Database Setup (PostgreSQL)

#### Install PostgreSQL
```bash
# macOS (using Homebrew)
brew install postgresql
brew services start postgresql

# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib

# Windows
# Download from https://www.postgresql.org/download/windows/
```

#### Create Database and User
```sql
-- Connect to PostgreSQL as superuser
psql postgres

-- Create database and user
CREATE DATABASE deel;
CREATE USER deel_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE deel TO deel_user;

-- Create schemas
\c deel
CREATE SCHEMA raw;
```

### 3. Load Raw Data

#### Load CSV files into PostgreSQL
```sql
-- Connect to deel database
\c deel

-- Load acceptance report
CREATE TABLE dbt_raw.globepay__acceptance_report AS
SELECT * FROM (VALUES (NULL::text, NULL::text, NULL::text, NULL::text, NULL::timestamp, NULL::text, NULL::text, NULL::numeric, NULL::text, NULL::text, NULL::text)) t(external_ref, status, source, ref, date_time, state, cvv_provided, amount, country, currency, rates) WHERE false;

\copy dbt_raw.globepay__acceptance_report FROM '/path/to/input_data/acceptance_report.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');

-- Load chargeback report  
CREATE TABLE dbt_raw.globepay__chargeback_report AS
SELECT * FROM (VALUES (NULL::text, NULL::text, NULL::text, NULL::boolean)) t(external_ref, status, source, chargeback) WHERE false;

\copy dbt_raw.globepay__chargeback_report FROM '/path/to/input_data/chargeback_report.csv' WITH (FORMAT csv, HEADER true);
```

### 4. Assignment Database Setup

**For this assignment, I have already configured a NeonDB PostgreSQL database with the required schemas and data loaded.** 

The database credentials and connection details are provided in a separate file for security purposes. The database includes:
- ✅ Pre-loaded raw data tables (`globepay__acceptance_report`, `globepay__chargeback_report`)
- ✅ All required schemas (`dbt_raw`, `dbt_stg`, `dbt_int`, `dbt_curated`)
- ✅ Proper permissions and user access configured

**To use the assignment database:**
1. Refer to the separate credentials file provided
2. Update/create the `~/.dbt/profiles.yml` in the home folder with the provided connection details
3. Skip the local PostgreSQL installation and data loading steps above
4. Proceed directly to running the EDA and dbt models

## 📊 Running the Analysis

### Step 1: Exploratory Data Analysis

#### Start Jupyter Notebook
```bash
# Ensure virtual environment is activated
source venv/bin/activate 
```

#### Open and Run EDA Notebook
1. Navigate to `eda_deel.ipynb` in the Jupyter interface
2. Run all cells sequentially (Cell → Run All)
3. Review insights on:
   - Data quality and completeness
   - Transaction patterns by country/currency
   - Acceptance rate analysis
   - Chargeback impact assessment
   - Temporal trends and seasonality

**Key EDA Outputs:**
- Data profiling and quality assessment
- Visualization of acceptance rates by geography
- Chargeback analysis and risk identification
- Currency exchange rate impact analysis

### Step 2: dbt Data Modeling

#### Configure dbt Profile
```bash
cd deel_dbt

# Update profiles.yml with your database credentials
cat > profiles.yml << EOF
deel_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: deel_user
      password: your_password
      port: 5432
      dbname: deel
      schema: public
      threads: 4
EOF
```

#### Install dbt Dependencies
```bash
# Install dbt packages
dbt deps --profiles-dir .
```

#### Load Seed Data
```bash
# Load calendar dimension
dbt seed --profiles-dir .
```

#### Run dbt Models
```bash
# Run all models (staging → intermediate → curated)
dbt run --profiles-dir .

# Run with specific selection
dbt run --select staging --profiles-dir .      # Staging layer only
dbt run --select intermediate --profiles-dir .  # Intermediate layer only
dbt run --select curated --profiles-dir .      # Curated layer only
```

#### Run Data Quality Tests
```bash
# Run all tests
dbt test --profiles-dir .

# Run tests for specific layers
dbt test --select staging --profiles-dir .
dbt test --select curated --profiles-dir .
```

#### Generate Documentation
```bash
# Generate and serve documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## 📈 Key Models and Outputs

### Staging Layer
- **`stg_globepay__acceptance_report`**: Cleaned transaction data with USD conversion
- **`stg_globepay__chargeback_report`**: Standardized chargeback data

### Intermediate Layer  
- **`int_transactions_with_chargeback`**: Enriched transactions with calendar and chargeback flags

### Curated Layer

#### Finance Domain
- **`fct_unrealized_chargebacks`**: Chargeback impact analysis by country and transaction age
- **`fct_declined_volume_by_country`**: Countries with >$25M declined volume

#### Performance Domain
- **`fct_acceptance_performance`**: Daily/weekly/monthly acceptance metrics with weekend analysis

#### Analytics Domain
- **`fct_tableau_dashboard_master`**: Comprehensive reporting table with 20+ KPIs for visualization

## 🧪 Data Quality & Testing

The project includes comprehensive data quality tests:

- **Column Tests**: Not null, uniqueness, accepted values
- **Business Logic Tests**: Acceptance rate bounds, transaction consistency
- **Cross-Model Tests**: Data freshness, volume consistency
- **Custom SQL Tests**: Advanced business rule validation

```bash
# View test results
dbt test --store-failures --profiles-dir .
```
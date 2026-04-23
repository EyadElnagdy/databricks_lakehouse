# Sales Lakehouse on Databricks (Bronze / Silver / Gold)

## 📌 Project Overview
This project demonstrates how to build a **Sales Lakehouse architecture** using **Databricks Lakehouse Platform** and **Delta Lake** tables.  
The solution follows the **Medallion Architecture** pattern with three layers:

- **Bronze Layer**: Raw ingestion (no transformations)
- **Silver Layer**: Cleaned, standardized, normalized data
- **Gold Layer**: Business-ready data model (Star Schema)

The data sources are **CSV files exported from ERP and CRM systems**, loaded in **batch full load mode**.

This project is intended for **training and learning purposes** using the **Databricks Free Edition** and **Unity Catalog**.

---

## 🏗️ Architecture
The lakehouse is structured into three main layers:

### 🥉 Bronze Layer (Raw Data)
- Loads raw CSV files into Delta tables
- No transformations are applied
- Data is stored exactly as received from source systems
- Load type: **Full Load (Overwrite)**

### 🥈 Silver Layer (Clean & Standardized Data)
- Reads data from Bronze Delta tables
- Applies cleaning and transformation logic:
  - Handling missing values
  - Removing duplicates (if applicable)
  - Standardizing column names and formats
  - Normalizing and standardizing data types
- Load type: **Full Load (Overwrite)**

### 🥇 Gold Layer (Business Layer / Star Schema)
- Reads cleaned data from Silver Delta tables
- Builds a **Star Schema** model optimized for analytics and BI:
  - **Fact Table**: `fact_sales`
  - **Dimension Tables**: `dim_customer`, `dim_product`
- Performs joins between fact and dimension entities
- Load type: **Full Load (Overwrite)**

---

## ⭐ Gold Data Model (Star Schema)

### Fact Table
- `fact_sales`: Contains transactional sales data (measures and foreign keys)

### Dimension Tables
- `dim_customer`: Customer master data
- `dim_product`: Product master data

This structure is designed to support reporting, dashboards, and analytical workloads.

---

## ⚙️ Technologies Used
- **Databricks Free Edition**
- **Apache Spark**
- **Delta Lake**
- **Unity Catalog**
- **Databricks Jobs (Scheduling / Orchestration)**
- **CSV Files** as source systems export format

---

## 📂 Repository Structure
```bash
├── docs/
│   ├── lakehouse_architecture.md
│   ├── layer_rules.md
│
├── scripts/
│   ├── bronze/
│   │   └── bronze_ingestion_notebook.py
│   ├── silver/
│   │   └── silver_transformation_notebook.py
│   ├── gold/
│   │   └── gold_star_schema_notebook.py
│
├── datasets/
│   ├── source_crm/
│   │   └── *.csv
│   ├── source_erp/
│       └── *.csv
│
└── README.md
```

📌 **docs/**  
Contains documentation related to the lakehouse design and rules for each layer.

📌 **scripts/**  
Contains Databricks notebooks for each layer (Bronze, Silver, Gold).

📌 **datasets/**  
Contains the raw source CSV datasets exported from ERP and CRM systems.

---

## 🔄 ETL Pipeline Workflow
The pipeline is executed sequentially using **Databricks Jobs**, where each layer depends on the successful execution of the previous one.

### Step 1: Bronze Ingestion Notebook
- Reads CSV files using Spark:
  ```python
  spark.read.csv(...)
  ```
- Writes raw data into **Bronze Delta tables** using overwrite mode.

### Step 2: Silver Transformation Notebook
- Reads Bronze Delta tables
- Cleans and standardizes the data
- Writes output into **Silver Delta tables** using overwrite mode

### Step 3: Gold Star Schema Notebook
- Reads Silver Delta tables
- Creates star schema tables (`fact_sales`, `dim_customer`, `dim_product`)
- Writes output into **Gold Delta tables** using overwrite mode

---

## 🕒 Scheduling & Orchestration
The pipeline is orchestrated using **Databricks Jobs**, with three scheduled tasks:

1. **Bronze Notebook Job**
2. **Silver Notebook Job**
3. **Gold Notebook Job**

Each task runs in order to ensure proper dependency management.

---

## 📥 Data Ingestion Details
- Source format: **CSV**
- Sources:
  - CRM exports (`datasets/source_crm/`)
  - ERP exports (`datasets/source_erp/`)
- Ingestion method: `spark.read.csv`
- Load strategy: **Batch Full Load**
- Write strategy: **Overwrite Delta tables**

---

## 🔐 Governance & Cataloging
This project uses **Unity Catalog** for managing:
- Data access control
- Table organization
- Centralized governance

All tables across Bronze, Silver, and Gold layers are stored as **Delta Lake tables** and registered in Unity Catalog.

---

## 🚀 How to Run the Project

### Prerequisites
- Databricks Free Edition account
- A Databricks workspace with Unity Catalog enabled
- Cluster running with Spark enabled

### Steps
1. Upload the dataset CSV files into the `datasets/` folder (or Databricks FileStore if needed).
2. Import the notebooks located in `scripts/bronze`, `scripts/silver`, and `scripts/gold` into Databricks.
3. Run notebooks in order:
   - Bronze notebook
   - Silver notebook
   - Gold notebook
4. (Optional) Create a Databricks Job and schedule the pipeline.

---

## 📊 Expected Output
After successful execution, the lakehouse will contain:

### Bronze Layer
- Raw Delta tables mirroring ERP/CRM exports

### Silver Layer
- Cleaned and standardized Delta tables

### Gold Layer
- `fact_sales`
- `dim_customer`
- `dim_product`

These Gold tables are ready to be consumed by BI tools such as Power BI or Tableau.

---

## 📝 Notes
- This project uses **full load overwrite strategy** in all layers.
- It is designed as a training implementation and does not include incremental ingestion or CDC logic.

---

## 📌 Author
Developed as a Databricks training project for building a complete Lakehouse solution using Medallion Architecture.

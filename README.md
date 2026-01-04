# 🏗️ SQL Data Warehouse Project (Bronze → Silver → Gold)

## 📌 Project Overview
This project demonstrates the design and implementation of a **modern SQL Server–based data warehouse** using a layered architecture:
- **Bronze** – Raw data ingestion
- **Silver** – Data cleansing and standardization
- **Gold** – Analytics-ready star schema

The solution transforms raw **CRM and ERP data** into high-quality datasets suitable for **business intelligence, reporting, and analytics**.

---

## 🧱 Architecture


---

## 📁 Repository Structure

├── bronze/
│ └── raw_tables.sql
├── silver/
│ ├── ddl_silver_tables.sql
│ ├── load_silver_procedure.sql
│ └── silver_data_quality_checks.sql
├── gold/
│ ├── ddl_gold_views.sql
│ ├── gold_data_quality_checks.sql
│ └── data_catalog_gold.md
└── README.md

---

## 🟤 Bronze Layer
### Purpose
Stores **raw, untransformed data** ingested from source systems.

### Key Characteristics
- No data cleaning or transformations
- Preserves source system structure
- Used as the ingestion foundation for Silver

---

## ⚪ Silver Layer
### Purpose
Transforms raw data into **clean, standardized, and trusted datasets**.

### Key Transformations
- Removal of duplicates using window functions
- Trimming unwanted spaces
- Standardization of gender, marital status, product lines, and countries
- Conversion of integer-based dates to proper `DATE` types
- Handling nulls, invalid values, and inconsistent sales calculations
- Addition of audit columns (`dwh_create_date`)

### ETL Implementation
- Centralized stored procedure: **`silver.load_silver`**
- Full refresh strategy using `TRUNCATE + INSERT`
- Execution time logging per table and batch

---

## 🟡 Gold Layer
### Purpose
Provides **analytics-ready datasets** modeled using a **star schema**.

### Gold Objects
- **Dimensions**
  - `gold.dem_customers`
  - `gold.dem_products`
- **Fact**
  - `gold.fact_sales`

### Design Principles
- Surrogate keys generated using `ROW_NUMBER()`
- Business-friendly naming conventions
- CRM treated as master data where applicable
- Only active products included
- Clean joins between facts and dimensions

---

## ⭐ Star Schema Design

       gold.dem_customers
               ↑
               |
          gold.fact_sales
               |
               ↓
        gold.dem_products

---

## 📊 Gold Layer Summary

### `gold.dem_customers`
- One row per customer
- Combines CRM data with ERP demographics and location
- CRM gender preferred; ERP used as fallback

### `gold.dem_products`
- One row per active product
- Enriched with category and sub-category hierarchy
- Excludes ended products

### `gold.fact_sales`
- One row per sales transaction
- Linked to customer and product dimensions
- Contains all sales measures and dates

---

## ✅ Data Quality & Validation

### Silver Layer
- Duplicate and NULL primary key checks
- Unwanted whitespace detection
- Categorical value standardization
- Date validity and logical ordering
- Sales calculation consistency checks

### Gold Layer
- Surrogate key uniqueness in dimensions
- Referential integrity between fact and dimensions
- Detection of orphan fact records

**Expectation:**  
All validation queries should return **zero rows**.

---

## 🛠️ Technologies Used
- SQL Server
- T-SQL
- Stored Procedures
- Views
- Window Functions
- Star Schema Modeling

---

## 🎯 Use Cases
- Business intelligence dashboards
- Sales performance analysis
- Customer analytics
- Product and category reporting
- Executive KPI reporting

---

## 🚀 Future Enhancements
- Incremental loading (CDC)
- Slowly Changing Dimensions (SCD)
- Metadata-driven ETL
- Automated data quality tests
- Performance tuning and indexing

---

## 👤 Author
**Muhib Jabbar**  
Data Analyst | Junior Data Engineer  
SQL • Data Warehousing • Analytics


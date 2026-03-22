# azure-data-engineering-project

##  Project Overview

This project demonstrates a complete end-to-end data engineering pipeline using Azure services. It follows the **Medallion Architecture (Bronze, Silver, Gold)** to process raw data into business-ready insights.

---

##  Technologies Used

* Azure Data Factory (ADF)
* Azure Data Lake Storage Gen2 (ADLS Gen2)
* Azure Databricks (PySpark)
* Azure Synapse Analytics
* SQL

---

##  Architecture Overview

![Architecture Diagram](architecture/architecture.png)

---

## 🔄 Data Pipeline Flow

1. **Data Source**

   * Data is ingested from Kaggle dataset.

2. **Data Ingestion (ADF)**

   * Azure Data Factory pipelines are used.
   * Implemented **Copy Activity** to ingest data.
   * Used **Lookup + ForEach (Dynamic Pipeline)** for scalable ingestion.

3. **Data Storage (ADLS Gen2)**

   * Data is stored in:

     * Bronze (Raw Data)
     * Silver (Cleaned Data)
     * Gold (Curated Data)

4. **Data Transformation (Databricks)**

   * Data is processed using **PySpark**.
   * Cleaning, transformation, and enrichment performed.
   * Output stored in Silver layer.

5. **Data Modeling & Analytics (Synapse)**

   * Created **External Tables** and **Views**.
   * Used OPENROWSET for querying data.
   * Final data available in Gold layer for analytics.

---

## ⚙️ Key Features

* ✅ End-to-End Data Pipeline
* ✅ Medallion Architecture Implementation
* ✅ Dynamic Pipelines using Lookup & ForEach
* ✅ Copy Activity for Data Ingestion
* ✅ PySpark Transformations in Databricks
* ✅ External Tables & Views in Synapse
* ✅ Scalable and Modular Design

---

## 📂 Project Structure

```
azure-data-engineering-project/
│
├── adf/
│   ├── ARMTemplateForFactory.json
│   └── ARMTemplateParametersForFactory.json
│
├── databricks/
│   └── silver_layer.py
│
├── synapse/
│   ├── schema.sql
│   ├── views.sql
│   └── external_tables.sql
│
├── architecture/
│   └── architecture.png
│
├── screenshots/
│   ├── adf_pipeline.png
│   ├── databricks.png
│   └── synapse.png
│
└── README.md
```

---

## 📸 Screenshots

### 🔹 ADF Pipeline

![ADF Pipeline](screenshots/adf_pipeline.png)

### 🔹 Databricks Transformation

![Databricks](screenshots/databricks.png)

### 🔹 Synapse Output

![Synapse](screenshots/synapse.png)

---

## 🚀 Business Use Case

This project simulates a **retail data analytics pipeline** where raw sales data is processed into structured, business-ready data to enable reporting and decision-making.

---

## 🔑 Keywords

Azure Data Engineer, ADF, ADLS Gen2, Databricks, Synapse, PySpark, ETL Pipeline, Medallion Architecture, Data Engineering Project

---

## 👩‍💻 Author

**Tejaswini Paritala**

---

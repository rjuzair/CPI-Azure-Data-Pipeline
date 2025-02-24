# Customer Retention Intelligence (CRI) - Azure Data Engineering Pipeline

## Project Overview
The **Customer Retention Intelligence (CRI)** project is a cloud-native data engineering solution built on Microsoft Azure to process, transform, and analyze customer retention and churn data from the AdventureWorks dataset (2015-2017). This pipeline ingests data, transforms it through a layered architecture, and prepares it for analytics, with visualization as a secondary focus using Power BI.

### Core Azure Components
- **Azure Data Factory (ADF)**: Orchestrates data ingestion and workflows.
- **Azure Data Lake Storage (ADLS) Gen2**: Stores raw, transformed, and aggregated data.
- **Azure Databricks**: Transforms data using PySpark.
- **Azure Synapse Analytics**: Performs serverless SQL analytics and aggregations.
- **Power BI**: Visualizes insights from processed data.

---

## Data Pipeline Architecture
The pipeline follows a **Bronze-Silver-Gold** layered architecture, depicted in the flowchart below:

![Data Architecture Flowchart](https://github.com/rjuzair/CPI-Azure-Data-Pipeline/blob/main/Data%20pipeline%20diagram.png)

### Data Flow
1. **Data Source**: Ingests data via HTTP (e.g., web APIs or file).
2. **Bronze Layer**: Raw data is stored in ADLS Gen2 via ADF.
3. **Silver Layer**: Databricks transforms and cleans data, storing it in ADLS Gen2.
4. **Gold Layer**: Synapse Analytics aggregates data for reporting, stored in ADLS Gen2.
5. **Visualization**: Power BI connects to the Gold layer for dashboards.

### Layer Details
| **Layer**       | **Purpose**                        | **Azure Service**         | **Storage Format** | **Key Activities**                              |
|-----------------|------------------------------------|---------------------------|--------------------|------------------------------------------------|
| **Bronze**      | Store raw data                     | Azure Data Factory        | CSV/Parquet        | Ingest data from HTTP sources.                 |
| **Silver**      | Clean and transform data           | Azure Databricks (PySpark)| Parquet            | Standardize formats, remove duplicates.        |
| **Gold**        | Aggregate data for analytics       | Azure Synapse Analytics   | Parquet            | Create aggregates (e.g., churn rates) via SQL. |

---

## Visualization with Power BI
Power BI dashboards include:
- **KPIs**: Churn Rate (%), Active Customers, Total Revenue ($).
- **Charts**: Funnel chart for churn risk.
- **Tables**: Customer trends with conditional formatting.
- **Filters**: Year slicer (2015-2017).

The pipeline is designed to support any BI tool, with Power BI as an example.

---

# CRI Pipeline Setup Instructions

## Setup Instructions

### Create ADLS Gen2 Account
1. Set up an **Azure Data Lake Storage (ADLS) Gen2** account.
2. Create the following containers within the ADLS Gen2 account:
   - `bronze`: For raw data storage.
   - `silver`: For transformed data storage.
   - `gold`: For aggregated data storage.

### Ingest Data
1. Use **Azure Data Factory (ADF)** to build a dynamic pipeline for data ingestion.
2. Locate the `.json` file in the `bronze/` folder, which contains links to all data sources (e.g., HTTP sources).
3. Configure ADF to use this `.json` file to dynamically ingest data into the `bronze` container.

### Transform Data
1. Use **Azure Databricks** to process data stored in the `bronze` container.
2. Navigate to the `silver_layer/` folder, which contains a Databricks notebook file.
3. Run the notebook to perform data cleaning, generate basic insights, and apply transformations.
4. Store the transformed results in the `silver` container.

### Aggregate Data
1. Use **Azure Synapse Analytics** to aggregate data for reporting.
2. Access the `gold_layer/` folder, which contains SQL scripts.
3. These SQL scripts:
   - Create views of data from the `silver` layer.
   - Perform aggregations and store the results in the `gold` layer using CETAS (Create External Table As Select) statements.
4. Execute these SQL scripts in Synapse to generate the aggregated tables.
   - Configuration files for Synapse SQL (if applicable) are also located in the `gold_layer/` folder.

### Visualize
1. Connect **Power BI** to the Azure Synapse SQL database.
2. Navigate to the `powerbi/` folder and load the `cri_dashboard.pbix` file.
3. Use the dashboard to visualize the aggregated data from the `gold` layer.

## Repository Contents
- `README.md`: This file.
- `flowchart.png`: Diagram of the data pipeline.
- `bronze/`:
  - `.json` file with links to all data sources (used to build the dynamic ADF pipeline).
- `silver_layer/`:
  - Databricks notebook file for data cleaning, insights, and transformations.
- `gold_layer/`:
  - SQL scripts to create views from the Silver layer, perform aggregations, and store data in the Gold layer using CETAS.
  - Configuration files for Synapse SQL (if applicable).
- `powerbi/`:
  -[`cri_dashboard.pbix`](https://app.powerbi.com/links/rOGhz2xjUn?ctid=913f18ec-7f26-4c5f-a816-784fe9a58edd&pbi_source=linkShare): Power BI dashboard file for visualization.

---

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

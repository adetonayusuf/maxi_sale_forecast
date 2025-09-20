## Customer Weather Demand Analysis - End-to-End Data Engineering Project

### Project Overview

This project demonstrates an end-to-end data engineering solution designed to analyze how weather patterns influence customer demand across different regions. The goal is to provide actionable business intelligence to improve decision-making related to sales, inventory, and marketing strategies. The entire data pipeline is orchestrated using Google Cloud Platform (GCP) and features a scalable, cost-effective architecture.

### Project Workflow

The project follows a robust data pipeline from raw data ingestion to business intelligence dashboards.

- Data Ingestion: Multi-source raw data (customer transactions and real-time weather data) is ingested.

- Orchestration & Infrastructure: The entire GCP infrastructure is provisioned and managed using Terraform for automation and reproducibility.

- Data Storage & Processing: Raw data is stored in Google Cloud Storage (GCS), and then processed in Snowflake. To optimize costs, data is accessed via Snowflake's external tables linked to GCS.

- Data Transformation: Data is transformed and enriched in Snowflake by joining customer transactions with weather data. Business logic is applied to flag demand signals (e.g., Normal, High, Low).

- Analytics & Visualization: The final analytical data is connected from Snowflake to Power BI, where interactive dashboards are created to provide key business insights.

### Data Architecture (Medallion Standard)

Our data pipeline is structured following the Medallion Architecture, a best-practice design pattern for building scalable and reliable data lakes.

- Bronze Layer (Raw): Raw, unvalidated data from the Customer Dump File and Weather API is stored as-is in Google Cloud Storage (GCS). This layer serves as the immutable historical record.  [Extraxt Data](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/extract_data_parquet.py)

- Silver Layer (Transformed): Data is cleansed, validated, filtered, and integrated within Snowflake. Here, we join customer transaction data with weather conditions and apply business logic to create initial demand signals and enriched datasets. [Transformed Date](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/Snowflake/maxiexternal.sql)

- Gold Layer (Business-Ready): This layer within Snowflake contains highly aggregated and refined datasets optimized for direct consumption. It includes final tables and views, optimized for performance and ease of use by Power BI dashboards.

![Data Architecture](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/Customer%20weather%20architecture1.gif)


### Key Features & Technologies

Infrastructure as Code: Terraform for provisioning and managing GCP resources (GCS, IAM, Networking).

- Data Sources:

    - Customer Transactions: A CSV dump file initially loaded into PostgreSQL for structured staging.

    - Weather Data: Real-time data (temperature, sunshine, conditions) extracted from an external API and stored in GCS.

- Cloud Platform: Google Cloud Platform (GCP).

- Databases: PostgreSQL for staging and Snowflake for advanced analytics and modeling (utilizing external tables for cost efficiency).

- Data Storage: Google Cloud Storage (GCS) for raw and processed data.

- Programming: Python for data ingestion, API calls, and pipeline automation.

- Business Intelligence: Power BI for creating comprehensive dashboards and visualizations.

### Business Insights Gained

Leveraging the integrated datasets and a robust analytical framework, the project delivers critical business insights:

- Weather-Driven Demand: Identified direct correlations between specific weather conditions (e.g., sunny days vs. colder days) and increased or decreased demand for certain product categories.

- Improved Forecasting Accuracy: The "Forecast Diagnostic" dashboard highlights forecasting performance, revealing areas for improvement and enabling better stock management.

- Regional Strategy Optimization: Analysis of "Regional & Weather Impact" helps understand how demand signals vary significantly across geographic regions, informing localized marketing and inventory strategies.

- Inventory Cost Optimization: The "Inventory & Risk" dashboard provides critical metrics like "Stockout Risk %" and "Overstock Risk %," enabling proactive adjustments to stock levels and reducing carrying costs.

- Product Performance Analysis: Detailed insights into product-level performance across various regions and weather conditions, aiding in product portfolio management.

### Power BI Dashboards

The project culminates in a suite of interactive Power BI dashboards, providing actionable insights:

Executive Overview

![Sum](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/sum.png)

: High-level KPIs on total units, actual and forecast revenue, forecast accuracy, and overall overstock risk. Includes actual vs. forecasted revenue trends and top-performing categories/products.

2.  Forecast Diagnostic

![Forecast Diagnostic](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/Forecast%20Diagnostic.png)

: Deep dive into forecast accuracy by product and store, showing MAPE (Mean Absolute Percentage Error) to identify specific products or locations with high forecasting variance.

3.  Inventory & Risk

![Inventory & Risk](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/Inventory%20%26%20Risk.png)

: Monitors total current stock, stockout risk, and overstock risk percentages. Provides detailed inventory levels by store and product, including days of stock left.

4.  Weather Impact & Drivers

![Weather](https://github.com/adetonayusuf/maxi_sale_forecast/blob/main/weathrer.png)

: Visualizes revenue by temperature and category, identifying how different weather conditions influence sales. Includes average temperature, sunshine hours, and category-specific forecast accuracy.

### Repository Structure


├── README.md                 # Project documentation

├── terraform/                # Terraform GCP provisioning scripts

├── src/                      # Python scripts for data ingestion and processing

│   ├── data_ingestion/       # ETL from dump → Postgres → GCS → Snowflake

│   └── transformations/      # SQL transformations in Snowflake
│
├── docs/                     # Documentation and project images (e.g., architecture diagram)

├── dashboards/               # Power BI dashboard files (.pbix)

└── requirements.txt          # Python dependencies

## Contact

Author: Yusuf Adetona

Email: yustone003@yahoo.com

LinkedIn: Yusuf Adetona HND, BSc, AAT, ACA, ACCA(Dip IFRS)

Portfolio: My Data Science Portfolio

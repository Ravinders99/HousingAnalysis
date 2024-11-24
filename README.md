Housing Analysis Project
This project is focused on analyzing Canadian housing data by leveraging Azure Cloud Services. It provides end-to-end data ingestion, transformation, machine learning, and visualization capabilities, supporting decision-making through interactive dashboards.

Features
Data Ingestion: Housing data is ingested from MongoDB into Azure using Azure Data Factory.
Data Transformation: Data is processed in Azure Databricks through multiple stages:
Bronze Layer: Raw ingested data.
Silver Layer: Cleaned and standardized data.
Gold Layer: Aggregated data ready for analysis.
Machine Learning: Predictive analytics and insights are performed on the transformed data in Databricks.
Data Storage: Transformed datasets are stored in Azure Data Lake Storage for further usage.
Visualization: Final insights are visualized using Power BI connected to the Gold Layer.
FastAPI Backend: Exposes APIs for querying housing data, images, and predictions.
Dockerized Deployment: The entire application is containerized for easy deployment on Azure App Service.
Architecture
Data Source: MongoDB stores the initial housing data.
Ingestion: Azure Data Factory moves data from MongoDB to Azure Data Lake Storage (Bronze layer).
Transformation: Azure Databricks performs data cleaning, feature engineering, and aggregation into Silver and Gold layers.
Machine Learning: Databricks runs predictive models for insights like price forecasting or affordability indices.
Visualization: Power BI connects to Azure Data Lake Storage to visualize housing data trends and machine learning results.
Backend: FastAPI exposes APIs for querying datasets and visualization integration.
Technologies Used
Azure Services
Azure Data Factory: For data ingestion from MongoDB to Azure Data Lake.
Azure Databricks: For data transformation (Bronze, Silver, Gold layers) and machine learning.
Azure Data Lake Storage: To store raw and transformed data.
Azure App Service: To deploy the FastAPI application.
Azure Container Registry (ACR): To manage Docker images.
Power BI: For interactive data visualization and reporting.
Other Technologies
FastAPI: Web framework for building APIs.
MongoDB: NoSQL database for initial data storage.
Docker: To containerize the application.
Python: For scripting, API development, and data processing.
Pandas: For data analysis and manipulation.
Pymongo: For MongoDB integration with Python.
Uvicorn: ASGI server to run FastAPI.
Project Workflow
Data Ingestion:

Data is ingested from MongoDB to Azure Data Lake (Bronze layer) using Azure Data Factory.
Custom ETL pipelines are created to handle scheduled ingestion.
Data Transformation:

Azure Databricks processes data in stages:
Bronze: Raw ingested data from MongoDB.
Silver: Cleaned data with standardized formats and fixed anomalies.
Gold: Aggregated data for machine learning and reporting.
Machine Learning:

Predictive models, such as price predictions, affordability indices, and trend analysis, are built in Azure Databricks.
Final predictions are stored in the Gold layer in Azure Data Lake.
Visualization:

Power BI connects directly to the Gold layer in Azure Data Lake to create interactive dashboards.
Reports include insights like average prices by region, facility availability, and tax affordability indices.
API Backend:

FastAPI provides endpoints to serve transformed data, predictions, and images for integration with external systems.


Run Locally
Start FastAPI server:

bash
Copy code
uvicorn main:app --reload
Run data ingestion script:

python mongodbingest.py

Generate artificial data:

python artificialData.py


Build Docker image:

docker build -t housing-analysis-app .


docker run -p 8000:8000 housing-analysis-app
Push Docker image to Azure ACR:

docker tag housing-analysis-app <acr-name>.azurecr.io/housing-analysis-app:v1
docker push <acr-name>.azurecr.io/housing-analysis-app:v1

Deploy the container to Azure App Service:
az webapp create --resource-group <resource-group-name> --plan <app-service-plan> --name <web-app-name> --deployment-container-image-name <acr-name>.azurecr.io/housing-analysis-app:v1


Images
![image](https://github.com/user-attachments/assets/1e5d9ce6-269a-4a9d-8943-34a765ac99b2)

![image](https://github.com/user-attachments/assets/59ed7198-6742-4b56-9727-1fbad53ce262)

![image](https://github.com/user-attachments/assets/d6c9e9a0-7c17-4c7f-a706-c561cdc8a554)

 Site link https://housingdata.azurewebsites.net/

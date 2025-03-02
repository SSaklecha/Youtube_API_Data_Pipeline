# YouTube Trending Data Pipeline on AWS

This project demonstrates an end-to-end data engineering solution for extracting, transforming, and analyzing YouTube trending video data using various AWS services. The pipeline automates data ingestion, transformation, cataloging, and loading into a data warehouse for analytics and visualization.

---

## Project Objective

- **Extract Data**: Retrieve trending video data across multiple regions from the YouTube Data API.
- **Transform Data**: Clean, format, and enrich raw data into an efficient, query-friendly format.
- **Load Data**: Store processed data in Amazon Redshift and S3 for scalable analytics.
- **Analytics & Visualization**: Enable ad-hoc SQL queries using Amazon Athena and build dashboards with Amazon QuickSight.
- **Orchestration**: Automate the workflow using AWS Step Functions and monitor with AWS CloudWatch.
- **Security & Governance**: Ensure data security and compliance via proper IAM roles, encryption, and logging.

---

## Pipeline Overview

![Youtube Data End-to-End Data Engineering Project Workflow](https://github.com/user-attachments/assets/2cbb5e08-d35c-4df9-ae8d-a0d91399ec0e)


1. **Cloud Setup & Management**  
   - Configure AWS environment and create IAM roles/policies with proper permissions.

2. **Data Extraction**  
   - AWS Lambda (scheduled weekly for testing) calls the YouTube Data API for trending videos across multiple regions.
   - Extracts key fields such as `video_id`, `trending_date`, `title`, `channel_title`, `category_id`, `publish_time`, `tags`, `views`, `likes`, `dislikes`, `comment_count`, `thumbnail_link`, `comments_disabled`, `ratings_disabled`, `video_error_or_removed`, `description`.
   - Prepares dimension data in JSON for region and category interpretation.
   - Cleans and transforms the data into CSV format.

3. **Landing**  
   - Store raw CSV/JSON data in an S3 landing bucket.

4. **Initial Cataloging**  
   - Use an AWS Glue Crawler to scan the landing bucket, infer the schema, and update the Glue Data Catalog.

5. **Data Transformation & Format Conversion**  
   - AWS Lambda fixes JSON format and converts data to efficient Parquet files.
   - AWS Glue ETL jobs adjust CSV data types, partition data by region, and join CSV with Parquet data.
   - A Spark job (via AWS Glue) filters, applies schema mapping, cleans data, and writes partitioned Parquet files back to S3.

6. **Final Data Movement**  
   - Execute a Glue ETL job to join processed data and load the final cleansed dataset into Amazon Redshift.

7. **Ad-Hoc Querying & Analytics**  
   - Use Amazon Athena for on-demand SQL queries directly against S3.
   - Utilize Amazon Redshift for scalable SQL-based exploration.

8. **Visualization**  
   - Build interactive dashboards and reports in Amazon QuickSight using data from Redshift.

9. **Orchestration & Monitoring**  
   - AWS Step Functions coordinate the entire pipeline.
   - Lambda functions trigger automatically on new data arrivals.
   - AWS CloudWatch monitors performance and sends alerts for any anomalies.

10. **Security & Governance**  
    - Enforce data security using IAM, encryption (S3/Redshift), and logging (CloudTrail/CloudWatch).
   
---
## Architecture Diagram
![Architecure Diagram](https://github.com/user-attachments/assets/aaed8d31-e93b-4fd7-8195-858c4465ce54)

---

## Getting Started

1. **Clone the Repository**

   ```bash
   git clone https://github.com/yourusername/youtube-trending-data-pipeline.git
   cd youtube-trending-data-pipeline


### AWS Setup

- **AWS Account & IAM Roles**:  
  Ensure you have an AWS account with proper permissions. Create IAM roles for AWS Lambda, Glue, Redshift, and other services using least-privilege principles.

- **S3 Buckets**:  
  Create a landing bucket for raw CSV/JSON data and a cleansed bucket for transformed, partitioned Parquet files.

- **AWS Glue Setup**:  
  Configure AWS Glue Crawlers to scan the landing bucket, infer schemas, and update the Glue Data Catalog. Set up AWS Glue ETL jobs for data transformation and conversion tasks.

- **Logging & Monitoring**:  
  Enable AWS CloudTrail and CloudWatch for logging API calls, monitoring performance, and setting up alerts.

### Configuration

- Update configuration files with your AWS credentials, region details, and S3 bucket names.
- Define the schedule (e.g., weekly for testing) for the AWS Lambda function responsible for data extraction.

### Deployment

- **Lambda Deployment**:  
  Deploy the AWS Lambda function that extracts data from the YouTube Data API.

- **Glue Jobs & Crawlers**:  
  Configure and deploy AWS Glue ETL jobs for cleaning, transforming, and converting data. Set up Glue Crawlers to maintain and update the Data Catalog as new data arrives.

- **Orchestration Setup**:  
  Set up AWS Step Functions to orchestrate the pipeline, triggering Lambda functions, Glue ETL jobs, and Redshift loading.

- **Data Warehouse**:  
  Create the necessary tables and schemas in Amazon Redshift and connect Amazon QuickSight to Redshift for visualization.

### Running the Pipeline

- Trigger the pipeline manually or wait for the scheduled execution of the AWS Lambda function. AWS Step Functions will manage the sequential execution of subsequent ETL and loading tasks.
- Monitor the pipeline progress using AWS CloudWatch logs and alarms, and review Step Functions execution details for success and error notifications.

---

## Project Structure

   ```bash
├── README.md                 # This file.
├── lambda_function/          # AWS Lambda function code for data extraction.
├── glue_jobs/                # AWS Glue ETL job scripts.
├── spark_jobs/               # Spark job scripts for data transformation.
└── docs/                     # Additional documentation and architecture diagrams.


```

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

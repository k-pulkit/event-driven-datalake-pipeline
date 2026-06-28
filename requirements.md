# Senior Systems Integrator Technology Services (AWS Cloud Data Engineer) – Take-Home Assignment

## Assignment Title:

**"Build a Secure, Scalable Data Pipeline on AWS Using Infrastructure as Code"**

### Business Scenario:

You’re leading a data engineering effort at a City agency. Each day, City-run transit vehicles upload trip data to a central S3 location. You are tasked with building a secure, production-grade data pipeline that transforms and analyzes this data using AWS services. The pipeline must be deployable via IaC and built with operational best practices in mind.

---

### Key Requirements:

#### 1. Data Ingestion

* Simulate uploading raw trip CSVs to an S3 bucket.
* Use S3 event triggers or scheduled jobs to kick off the pipeline.

#### 2. Data Processing & Cataloging

* Create a Glue crawler to catalog raw data.
* Write a Glue PySpark job to:
* Clean and normalize the data
* Convert CSV to Parquet
* Partition by date



#### 3. Data Querying

* Configure Athena to query processed data.
* Create SQL scripts for:
* Daily ridership summary
* Top routes over past 7 days
* Any outlier detection you design



#### 4. Data Aggregation into RDS

* Create a PostgreSQL or MySQL RDS instance.
* Load pre-aggregated data into RDS tables using Glue ETL or custom scripts.

#### 5. Infrastructure as Code (IaC)

* Use either CloudFormation, Terraform, or AWS CDK to deploy:
* S3 buckets
* Glue resources
* IAM roles
* RDS (optionally skip RDS subnet/VPC config for simplicity)
* Athena workgroup setup



#### 6. Orchestration

* Design a Step Functions or Glue workflow.
* Demonstrate orchestration of: `crawler → ETL → load → notification`

#### 7. Security & Access Control

* Follow least privilege principle for IAM roles.
* Enable encryption at rest (S3, Glue, RDS).
* Ensure access logging is configured (at least described).

#### 8. Monitoring & Logging

* Enable CloudWatch logs for Glue jobs.
* Describe how you would set up alarms for ETL failures or high-cost Athena queries.

#### 9. CI/CD Awareness (Optional)

* **Bonus:** Include a simple GitHub Actions or CodePipeline YAML to deploy the stack.

---

### Deliverables:

Please create and prepare a **maximum 15-minute PowerPoint presentation** including the following:

1. **Professional Introduction:**
* Describe how your work experience, skills, education, and training will make you successful in the role of Senior Systems Integrator Technology Services (AWS Cloud Data Engineer) in the Technology Services Division.


2. **Technical Demonstrations (Based on the assignment details):**
* **IaC Code:** Terraform, CDK, or CFN (full stack or module form).
* **Glue Scripts & SQL Queries:** PySpark scripts and Athena analysis queries.
* **Architecture Diagram:** PDF or embedded image.
* **README containing:**
* Architecture explanation
* Deployment instructions
* Assumptions and design trade-offs
* Future extensions (e.g., Redshift, Lake Formation, multi-account setup, etc.)
* *(Optional)* Screenshots or CLI logs if a real deployment was completed.

### Presentation Notes:

* You will be responsible for sharing the presentation over **MS Teams in presentation mode** for ease of viewing by panel members.
* Grading will be based on the **content of your presentation, your delivery, grammar, and speaking notes**.
* Your presentation will be followed by a **Q&A session** with the panel.
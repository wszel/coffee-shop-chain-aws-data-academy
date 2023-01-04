# Coffee shop chain - AWS Data Academy
 
The project of data analysys pipeline on a fictional coffee chain retail data. 
Data for a coffee shop chain with three locations in New York City. 
 
 Includes:
- key metrics
- case presentation
- user stories
- architecture 
- data modeling
- architecture (AWS)
- data visualisation (PowerBI)


## Client would like to 

- recieve weekly reports
- discover key trends in the business and gain insight from the data to increase sales and open a new point in the Coffee Shop Chain
- store all the data in one place with authorised ccess 


The end-user point of view example:

![clients-needs](https://user-images.githubusercontent.com/38794956/210568335-f99e4e1e-60d4-44cc-8da5-d8413f557b4e.png)


## Key metrics

- revenue
- labour cost percentage (lcp)
- cost of goods sold (cogs)
- fixed costs
- gross margin
- customer retention rate
- employee retention rate


## User stories 

User stories were created to describe final user wants in non-technical way.
The user story example: 

![user-story-reliability](https://user-images.githubusercontent.com/38794956/210566767-5f0a45b8-7dd6-483c-a050-09dbdbe0b48c.png)


## Database schema

The data was organised into three .csv files: Sales Reciept, Pastry Inventory snd Sales Target. 
The data was modeled with the following approach: 

![data-modeling](https://user-images.githubusercontent.com/38794956/210567402-a289ed09-3a45-4872-95cf-21c052147726.png)



## Architecture

The data pipeline was created with the use of Amazon Web Services. 
The schema below shows architecture model. 

![architecture-aws](https://user-images.githubusercontent.com/38794956/210565719-b56dd1cb-a979-4898-853f-d42b7a3e181a.png)


### Amazon S3
All files were stored in Amazon S3 buckets in .csv, .parquet and xlsx formats. 
The following buckets have been defined:
- raw data - original files
- clean data - partitioned by date files 
- conform data - tables created according to database schema 
- reports - xlsx reports

### Amazon Glue Services 
#### Crawlers
Map data from S3 buckets to Databases to be transformed further.

####Databases
Hold all mapped tables.

#### Jobs
Perform transformations on data:
- partition data
- create tables
- create relations between tables

#### Amazon Athena
Executes SQL queries on data tables.

Queries have been created for following topics :
- customer report
- employee report
- sales report

#### Amazon Lambda
Runs scripts in a serverless mode to perform simple tasks:
- check s3 files - checks whether all necessary files have been uploaded to S3 bucket
- start crawler - invokes execution of a crawler
- check crawler status - checks whether crawler run has finished
- create report - creates an .xlsx report using SQL queries
- send mail - sends a notification that the process is finished

## Data Visualisation 
Samples of reports were done using PowerBI tool. 

- weekly sales report 

![weekly sales report](https://user-images.githubusercontent.com/38794956/210564376-2083513d-8c49-453f-b59f-542659497c00.png)



- customers report

![customers report](https://user-images.githubusercontent.com/38794956/210564644-ef7c9b9d-c2d8-4794-b7e1-61c52b235e5e.png)



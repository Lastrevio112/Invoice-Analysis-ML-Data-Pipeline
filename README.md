# Scanned Invoices ML Data Pipeline | (BigQuery, Python, dbt, GitHub Actions, Docker, Modal, R2, Streamlit)
An end-to-end data pipeline that parses image scans of invoices from multiple data sources and loads them into a data warehouse for BI and spend analytics.

# The deployed app
CLICK HERE TO SEE THE FRONT END ON STREAMLIT (END RESULT OF THIS PROJECT):

https://invoice-analysis-ml-data-pipeline.streamlit.app/

# Summary/Description
This is an end-to-end data pipeline that polls multiple R2 buckets for any new input files, which can be PDFs or images of scanned invoices.

If a new invoice is found, a call is made to a pre-trained ML model (Docling) that's deployed on the cloud (on Modal) to extract JSON data out of the image. 

It is then processed by BigQuery through dbt in a medallion architecture accross all three layers (bronze -> silver -> gold), orchestrated by Python.

GitHub actions was used for both orchestration scheduling and for CI/CD.

The front end BI/dashboard was created in Streamlit, and the link to the app can be accessed from above for anyone to see.

# DATA FLOW EXPLAINED

The raw data (images, PDFs) reside in multiple R2 buckets, each of them representing a separate data source (ds1, ds2). At the moment, I integrated only two data sources, each corresponding to a public dataset of scanned invoices.

**- CDC explained -**

Each R2 bucket has three sub-directories: unprocessed, startedprocessing, processed.

The main script orchestrating this pipeline is process_new_files_for_all_ds.py, found in the modal/ sub-folder of this repo. It polls the unprocessed/ directory of every data source bucket every 15 minutes (scheduled by a cron job on GitHub actions) and if any files are found, they are instantly moved to startedprocessing/. When the pipeline ends, the file is moved from startedprocessing/ to processed/. 

This is how we implement change-data-capture in a safe way while maintaining all our raw data. If a file is in unprocessed/, it is up for either processing or retry. If a file is in startedprocessing/, it means that an attempt was made to process it but it failed midway, so a second pipeline can periodically move files from startedprocessing/ back to unprocessed/. If a file is in processed/, then it has been inserted succesfully in our data warehouse and that registry can be kept asa source of all our raw historical data.

**- Machine Learning & Exception-Handling Logic -**

After the file is moved to startedprocessing/, the PDF or .jpg file is converted to bytecode and passed onto a function deployed on modal.com calling a pre-trained open source machine learning model (Docling) that extracts structured JSON information out of that image. Each data source has a different layout of the image and therefore will output a JSON with different fields or a different structure. The expected structure of the JSONs are defined in Pydantic classes stored in modal/pydantic_models/ in this repo.

If the returned JSON is valid, the JSON is inserted into BigQuery's bronze layer tables. After that, our main Python script calls dbt run and dbt test, and if both pass, the file is moved from startedprocessing/ to processed/.

If the JSON is not valid, the processing for this file fails and the other files in the batch are attempted to be processed anyway. But if dbt run or dbt test fail, the entire process is aborted.

There is a second pipeline under modal/cleanupstartedprocessing_folder.py that periodically (once a day at midnight) checks if any files are in startedprocessing/ that have been there for over an hour and if yes, it moves them back to unprocessed/ for retry.

**- dbt data flow explained -**

dbt moves this data accross three layers. After data gets in bronze, the JSON is flattened and the arrays are unnested using BigQquery's native JSON functions; unique surrogate keys are generated and the columns are safely cast to their correct data types. Then data is moved into the 'silver layer' (intermediary) tables (which are different for each data source) with an *upsert* merge data loading type ("incremental" in DBT).

Finally, we have a star schema in the gold layer which is data source agnostic: the main fact table (fact_invoice_line) stores information at the granularity of an invoice line while dim_buyer and dim_vendor store information (name, address) about each buyer and each seller that can appear on the raw invoices.

**- CI/CD -**

GitHub actions was used not only for scheduling the orchestration scripts written in Python, but also for **CI/CD**. Whenever a change is made to Pydantic models and/or the invoice_to_json.py script (which contains the logic of the ML model on Modal), the model is re-deployed to modal again to keep it up to date with the new code without me needing to manually deploy it in Powershell. And whenever there is any change made to the dbt/ folder in this repo (that is pushed to GitHub), dbt test runs automatically.

**- Front end -**

Streamlit was used for the front-end dashboarding/data visualization. Even though Streamlit is not a BI tool, I made an attempt to simulate the equivalent of a 'semantic model' by caching data from BigQuery in memory every 24 hours, creating a sidebar of global filters on the left side of the dashboard and having each chart check those filters.

**- Docker -**

Finally, the entire project was containarized using Docker, with VS Code dev containers. All the Docker-related code can be found in .devcontainer.

# DATA MODEL:

Data model for data source 1:
<img width="626" height="673" alt="image" src="https://github.com/user-attachments/assets/15cc85e0-2a24-4591-80b9-7fa472581e20" />

Data model for data source 2:
<img width="662" height="682" alt="image" src="https://github.com/user-attachments/assets/416e2559-6887-4480-a17a-16970aca9f03" />

# SOURCES:

Data for data source 1 taken from here: https://huggingface.co/datasets/katanaml-org/invoices-donut-data-v1/viewer/default/validation
Data for data source 2 taken from here: https://github.com/femstac/Sample-Pdf-invoices

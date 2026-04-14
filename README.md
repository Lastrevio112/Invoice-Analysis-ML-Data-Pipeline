# Scanned Invoices ML Data Pipeline | (BigQuery, Python, dbt, GitHub Actions, Modal, R2, Streamlit)
An end-to-end data pipeline that parses image scans of invoices and loads them into a data warehouse for BI and spend analytics.

# The deployed app
CLICK HERE TO SEE THE FRONT END ON STREAMLIT (END RESULT OF THIS PROJECT):

https://invoice-analysis-ml-data-pipeline.streamlit.app/

# Summary/Description
This is an end-to-end pipeline that polls an R2 bucket for any new input files, which can be PDFs or images of scanned invoices.

If a new invoice is found, a call is made to a pre-trained ML model (Docling) that's deployed on the cloud (on Modal) to extract JSON data out of the image. 

It is then processed by BigQuery through dbt in a medallion architecture accross all three layers (bronze -> silver -> gold), orchestrated by Python.

GitHub actions was used for both orchestration scheduling and for CI/CD.

The front end BI/dashboard was created in Streamlit, and the link to the app can be accessed from above for anyone to see.


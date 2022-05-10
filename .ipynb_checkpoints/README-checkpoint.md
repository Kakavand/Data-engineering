# Data-engineering
EU-water-engineering

In this there is a ETL pipeline that is prepared form the public dataset;

https://data.europa.eu/euodp/en/data/dataset/DAT-163-en ;

1. In GCP the built in message broker Pub/Sub is used instead of Kafka, to extract data to publish and subscribe.

2. A data quality check is done, in a notebook.

3. Then a scheduler using Airflow is used to schedule the pipline. Airflow is also used to publish the data. Export the data to BigQuery, for analysis.

4. The data is later exported into Avro format.

5. The data is then used to create a report in Data Studio. See link below;

https://datastudio.google.com/reporting/84c1f7e9-1fc5-491c-852c-fa42083950b0



Basic principles of data engineering and practices are demonstrated in this assignment.

10th-May-2022

Shahow Kakavandy









# AQIPulse: Tracking Global Air Quality
> Which of the world's major cities have the cleanest and dirtiest air —
> and how is that changing over time?
## Stack
- OpenAQ API — daily pollutant data ingestion
- Python — fetch, parse, and load directly into Databricks
- Apache Spark (PySpark) on Databricks — silver transformation
- dbt — city rankings, pollutant mix, and trend modeling
- Looker Studio — interactive air quality dashboard
## Status
In progress
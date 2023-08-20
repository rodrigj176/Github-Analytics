# Github-Analytics
Full-stack final project for Big Data Systems.

## System Architecture
- A stream processing pipeline contained within a docker container specified by docker compose.

- Data comes from github http requests and is organized by the data source. It is then sent to a spark cluster through a TCP
stream.

- The queries are then processed in a redis dataframe, and are converted to a
dictionary to analyze the results in my flask web app.

How to run:

$ docker-compose up

$ docker exec streaming_spark_1 spark-submit /streaming/spark_app.py

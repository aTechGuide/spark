## Spark SQL

This repository contains Spark SQL Scripts.

# Spinning Docker Cluster
- `docker-compose up --scale spark-worker=3`
- `docker exec -it <IP of Master> bash` | `docker exec -it spark-cluster_spark-master_1 bash`
- `cd spark`
- `./bin/spark-sql`

## References
- This project is build as part of [rockthejvm.com  spark-streaming](https://rockthejvm.com/p/spark-streaming) Course
- Data used in this repository is taken from
  - [Apache Spark Repo Examples](https://github.com/apache/spark/tree/master/examples/src/main/resources)
  - [Apache spark with scala hands on with big data Course Material](https://www.udemy.com/apache-spark-with-scala-hands-on-with-big-data/)

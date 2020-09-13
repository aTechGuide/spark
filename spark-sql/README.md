## Spark SQL

This repository contains Spark SQL Scripts.

# Spinning Docker Cluster
- Go to `spark-cluster` folder
- Build docker images `./build-images.sh`
- Spin up the cluster `docker-compose up -d --scale spark-worker=3`
- Graceful stopping and removing containers `docker-compose down`
- Go inside the master node `docker exec -it <IP of Master> bash` | `docker exec -it spark-cluster_spark-master_1 bash`
- `cd spark`
- `./bin/spark-sql`
- Copying Data inside container `cp src/main/resources/data/ spark-cluster_spark-master_1:/tmp`

## URLs
- [Spark UI](http://localhost:4040/jobs/)

## References
- This project is build as part of 
  - [rockthejvm.com Spark Essentials with Scala](https://rockthejvm.com/p/spark-essentials) Course
  - [rockthejvm.com Spark Optimization with Scala](https://rockthejvm.com/p/spark-optimization) Course | [GIT rockthejvm/spark-optimization](https://github.com/rockthejvm/spark-optimization)
- Data used in this repository is taken from
  - [Apache Spark Repo Examples](https://github.com/apache/spark/tree/master/examples/src/main/resources)
  - [Apache spark with scala hands on with big data Course Material](https://www.udemy.com/apache-spark-with-scala-hands-on-with-big-data/)

## Spark Scripts

This repository contains simple spark scripts

### Commands
- First run the command ` export TERM=xterm-color`
- To build Jar, Run `sbt assembly`
- To view content of Jar file, `jar tf target/scala-2.11/MovieRating-assembly-1.0.jar`
- To Run Jar file,
  - `spark-submit --class=in.kamranali.movies.RatingsCounter target/scala-2.11/SparkScripts-assembly-0.1.jar`
  - `spark-submit --class=in.kamranali.customerOrder.PurchaseByCustomer  target/scala-2.11/SparkScripts-assembly-0.1.jar`
  
  
## Reference
- [Apache spark with scala hands on with big data](https://www.udemy.com/apache-spark-with-scala-hands-on-with-big-data/learn/v4/t/lecture/5364972?start=0)
- [Movielens Datasets](https://grouplens.org/datasets/movielens/)


## Notes
- If we have data that is very large and can't fit in memory + we don't want to send that data across network more than once we can use broadcast variables
  - We can take that chunck of data and will explicitey send it to all the nodes of the cluster. So that its there and ready when even anone needs it
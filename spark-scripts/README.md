## Spark Scripts

This repository contains simple spark scripts

### Commands
- First run the command ` export TERM=xterm-color`
- To build Jar, Run `sbt assembly`
- To view content of Jar file, `jar tf target/scala-2.11/MovieRating-assembly-1.0.jar`
- To Run Jar file,
  - `spark-submit --class=in.kamranali.movies.RatingsCounter target/scala-2.11/SparkScripts-assembly-0.1.jar`
  - `spark-submit --class=in.kamranali.customerOrder.PurchaseByCustomer  target/scala-2.11/SparkScripts-assembly-0.1.jar`
  - `spark-submit --class=in.kamranali.movies.MovieSimilarities target/scala-2.11/SparkScripts-assembly-0.1.jar 50`
  
  
## Reference
- [Apache spark with scala hands on with big data](https://www.udemy.com/apache-spark-with-scala-hands-on-with-big-data/)
- [Movielens Datasets](https://grouplens.org/datasets/movielens/)
- [Salary Data from Superdatascience Dataset](https://www.superdatascience.com/machine-learning/)


## Notes
- **Broadcast Variables** If we have data that is very large and can't fit in memory + we don't want to send that data across network more than once we can use broadcast variables
  - We can take that chunck of data and will explicitey send it to all the nodes of the cluster. So that its there and ready when even anone needs it
- **Accumulator** allows many executors to increment a shared variable
- **Item Based Collaborative Filtering**
  - For same pair of movies, if they got similar ratings from many users we say these movies are related to each other
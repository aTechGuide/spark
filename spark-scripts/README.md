## Spark Scripts

This repository contains simple spark scripts

### Commands
- First run the command ` export TERM=xterm-color`
- To build Jar, Run `sbt assembly`
- To view content of Jar file, `jar tf target/scala-2.11/MovieRating-assembly-1.0.jar`
- To Run Jar file,
  - `spark-submit --class=in.kamranali.basics.RatingsCounter target/scala-2.11/SparkScripts-assembly-0.1.jar`
  - `spark-submit --class=in.kamranali.basics.PurchaseByCustomer  target/scala-2.11/SparkScripts-assembly-0.1.jar`
  - `spark-submit --class=in.kamranali.movies.MovieSimilarities target/scala-2.11/SparkScripts-assembly-0.1.jar 50`

## Notes
- **Broadcast Variables** If we have data that is very large and can't fit in memory + we don't want to send that data across network more than once we can use broadcast variables
  - They are read only variables present in-memory cache on every machine
  - It eliminates the necessity to ship copies of variable for every task so that processing can be faster
  - We can take that chunck of data and will explicitey send it to all the nodes of the cluster. So that its there and ready when even anone needs it
- **Accumulator** allows many executors to increment a shared variable
- **Windowed Computation** implies transformation on RDDs are applied over a sliding window of data. The RDDs that fall under a particular window are combined and operated upon to produce new RDDs. It has two main parameters
  - *Window Length* The duration of window
  - *Sliding Interval*  The duration at which the windowed operation is performed
- **Item Based Collaborative Filtering**
  - For same pair of movies, if they got similar ratings from many users we say these movies are related to each other
  
## Reference
- [Apache spark with scala hands on with big data](https://www.udemy.com/apache-spark-with-scala-hands-on-with-big-data/)
- [Movielens Datasets](https://grouplens.org/datasets/movielens/)
- [Salary Data from Superdatascience Dataset](https://www.superdatascience.com/machine-learning/)
- [Edureka Top apache Spark Questions](https://www.edureka.co/blog/interview-questions/top-apache-spark-interview-questions-2016/)
  

## Spark Scripts

### Commands
- First run the command ` export TERM=xterm-color`
- To build Jar, Run `sbt assembly`
- To view content of Jar file, `jar tf target/scala-2.11/MovieRating-assembly-1.0.jar`
- To Run Jar file,
  - `spark-submit --class=in.kamranali.RatingsCounter target/scala-2.11/SparkScripts-assembly-0.1.jar`
  - `spark-submit --class=in.kamranali.PurchaseByCustomer  target/scala-2.11/SparkScripts-assembly-0.1.jar`
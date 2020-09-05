package atech.guide.concepts

/**
  * Reference
  * - https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-window-functions-7b4e39ad3c86
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowAggregateFunction extends App {

  private val spark = SparkSession.builder()
    .appName("WindowAggregateFunction")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  // Creating Data
  case class Salary(depName: String, empNo: Long, salary: Long)

  import spark.implicits._

  val empsalary = Seq(
    Salary("sales", 1, 5000),
    Salary("personnel", 2, 3900),
    Salary("sales", 3, 4800),
    Salary("sales", 4, 4800),
    Salary("personnel", 5, 3500),
    Salary("develop", 7, 4200),
    Salary("develop", 8, 6000),
    Salary("develop", 9, 4500),
    Salary("develop", 10, 5200),
    Salary("develop", 11, 5200)
  ).toDF()

  /**
    * Get Max / Min of a Column
    * +---------+---------+
    * |MaxSalary|MinSalary|
    * +---------+---------+
    * |     6000|     3500|
    * +---------+---------+
    */
  empsalary
    .agg(max("salary") as "MaxSalary", min("salary") as "MinSalary")
//    .show


  /**
    * Apply aggregate function on window to find the max and min salary in each department
    *
    * - We first create a partition of the data on department name
    * - aggregate function will be applied to each partition and return the aggregated value (min and max in our case.)
    *
    * +---------+-----+------+----------+----------+
    * |  depName|empNo|salary|max_salary|min_salary|
    * +---------+-----+------+----------+----------+
    * |  develop|    7|  4200|      6000|      4200|
    * |  develop|    8|  6000|      6000|      4200|
    * |  develop|    9|  4500|      6000|      4200|
    * |  develop|   10|  5200|      6000|      4200|
    * |  develop|   11|  5200|      6000|      4200|
    * |    sales|    1|  5000|      5000|      4800|
    * |    sales|    3|  4800|      5000|      4800|
    * |    sales|    4|  4800|      5000|      4800|
    * |personnel|    2|  3900|      3900|      3500|
    * |personnel|    5|  3500|      3900|      3500|
    * +---------+-----+------+----------+----------+
    */

  // Creating specification of Window e.g. partition data based on department
  val byDeptAgg = Window.partitionBy("depName")

  empsalary
    .withColumn("max_salary", max("salary").over(byDeptAgg))
    .withColumn("min_salary", min("salary").over(byDeptAgg))
    // .show

  /**
    * Window Ranking Function
    *
    * Question: To rank the employees based on their salary within the department
    */

  val byDeptOrder = byDeptAgg.orderBy($"salary".desc)

  /**
    * Rank function
    *
    * - It will return the rank of each record within a partition and skip the subsequent rank following any duplicate rank
    * - we can see some of the ranks are duplicated and some ranks are missing
    *   because the rank function will keep the same rank for the same value and skip the next ranks accordingly
    *
    * +---------+-----+------+----+
    * |  depName|empNo|salary|rank|
    * +---------+-----+------+----+
    * |  develop|    8|  6000|   1|
    * |  develop|   10|  5200|   2|
    * |  develop|   11|  5200|   2|
    * |  develop|    9|  4500|   4|
    * |  develop|    7|  4200|   5|
    * |    sales|    1|  5000|   1|
    * |    sales|    3|  4800|   2|
    * |    sales|    4|  4800|   2|
    * |personnel|    2|  3900|   1|
    * |personnel|    5|  3500|   2|
    * +---------+-----+------+----+
    */

  empsalary
    .withColumn("rank", rank().over(byDeptOrder))
    // .show

  /**
    *
    * Dense Rank
    * - It will return the rank of each record within a partition but will not skip any rank
    * - We can see some of the ranks are duplicated, but ranks are not missing
    *
    * +---------+-----+------+----------+
    * |  depName|empNo|salary|dense_rank|
    * +---------+-----+------+----------+
    * |  develop|    8|  6000|         1|
    * |  develop|   10|  5200|         2|
    * |  develop|   11|  5200|         2|
    * |  develop|    9|  4500|         3|
    * |  develop|    7|  4200|         4|
    * |    sales|    1|  5000|         1|
    * |    sales|    3|  4800|         2|
    * |    sales|    4|  4800|         2|
    * |personnel|    2|  3900|         1|
    * |personnel|    5|  3500|         2|
    * +---------+-----+------+----------+
    *
    *
    */

  empsalary
    .withColumn("dense_rank", dense_rank().over(byDeptOrder))
//    .show

  /**
    *
    * Row Number
    * - It will assign the row number within the window.
    * - If 2 rows will have the same value for ordering column, it is non-deterministic which row number will be assigned to each row with same value
    *
    * +---------+-----+------+----------+
    * |  depName|empNo|salary|row_number|
    * +---------+-----+------+----------+
    * |  develop|    8|  6000|         1|
    * |  develop|   10|  5200|         2|
    * |  develop|   11|  5200|         3|
    * |  develop|    9|  4500|         4|
    * |  develop|    7|  4200|         5|
    * |    sales|    1|  5000|         1|
    * |    sales|    3|  4800|         2|
    * |    sales|    4|  4800|         3|
    * |personnel|    2|  3900|         1|
    * |personnel|    5|  3500|         2|
    * +---------+-----+------+----------+
    *
    */

  empsalary
    .withColumn("row_number", row_number().over(byDeptOrder))
//      .show

  /**
    *
    * Percent Rank Function
    * - It will return the relative (percentile) rank within the partition
    *
    * +---------+-----+------+------------+
    * |  depName|empNo|salary|percent_rank|
    * +---------+-----+------+------------+
    * |  develop|    8|  6000|         0.0|
    * |  develop|   10|  5200|        0.25|
    * |  develop|   11|  5200|        0.25|
    * |  develop|    9|  4500|        0.75|
    * |  develop|    7|  4200|         1.0|
    * |    sales|    1|  5000|         0.0|
    * |    sales|    3|  4800|         0.5|
    * |    sales|    4|  4800|         0.5|
    * |personnel|    2|  3900|         0.0|
    * |personnel|    5|  3500|         1.0|
    * +---------+-----+------+------------+
    */

  empsalary
    .withColumn("percent_rank", percent_rank().over(byDeptOrder))
//    .show

  /**
    * N-tile Function
    * - This function can further sub-divide the window into n groups based on a window specification or partition
    * - For example, if we need to divide the departments further into say three groups we can specify ntile as 3
    *
    * +---------+-----+------+------------+
    * |  depName|empNo|salary|ntile       |
    * +---------+-----+------+------------+
    * |  develop|    8|  6000|           1|
    * |  develop|   10|  5200|           1|
    * |  develop|   11|  5200|           2|
    * |  develop|    9|  4500|           2|
    * |  develop|    7|  4200|           3|
    * |    sales|    1|  5000|           1|
    * |    sales|    3|  4800|           2|
    * |    sales|    4|  4800|           3|
    * |personnel|    2|  3900|           1|
    * |personnel|    5|  3500|           2|
    * +---------+-----+------+------------+
    */
  empsalary
    .withColumn("ntile", ntile(3).over(byDeptOrder))
//      .show

  /**
    * Window analytical functions
    */

  /**
    * Cumulative distribution function
    *
    * - It gives the cumulative distribution of values for the window/partition
    *
    * +---------+-----+------+------------------+
    * |  depName|empNo|salary|      cume_dist   |
    * +---------+-----+------+------------------+
    * |  develop|    8|  6000|               0.2|
    * |  develop|   10|  5200|               0.6|
    * |  develop|   11|  5200|               0.6|
    * |  develop|    9|  4500|               0.8|
    * |  develop|    7|  4200|               1.0|
    * |    sales|    1|  5000|0.3333333333333333|
    * |    sales|    3|  4800|               1.0|
    * |    sales|    4|  4800|               1.0|
    * |personnel|    2|  3900|               0.5|
    * |personnel|    5|  3500|               1.0|
    */

  empsalary
    .withColumn("cume_dist", cume_dist().over(byDeptOrder))
//    .show

  /**
    * Lag function
    *
    * - It will return the value prior to offset rows from DataFrame.
    *
    * The lag function takes 3 arguments (lag(col, count = 1, default = None)),
    * - col: defines the columns on which function needs to be applied.
    * - count: for how many rows we need to look back.
    * - default: defines the default value.
    *
    * +---------+-----+------+----+
    * |  depName|empNo|salary| lag|
    * +---------+-----+------+----+
    * |  develop|    8|  6000|null|
    * |  develop|   10|  5200|null|
    * |  develop|   11|  5200|6000|
    * |  develop|    9|  4500|5200|
    * |  develop|    7|  4200|5200|
    * |    sales|    1|  5000|null|
    * |    sales|    3|  4800|null|
    * |    sales|    4|  4800|5000|
    * |personnel|    2|  3900|null|
    * |personnel|    5|  3500|null|
    * +---------+-----+------+----+
    */

  empsalary
    .withColumn("lag", lag($"salary", 2).over(byDeptOrder))
//      .show

  /**
    * Lead Function
    *
    * - It will return the value after the offset rows from DataFrame.
    *
    * lead function takes 3 arguments (lead(col, count = 1, default = None))
    * - col: defines the columns on which the function needs to be applied.
    * - count: for how many rows we need to look forward/after the current row.
    * - default: defines the default value.
    *
    * +---------+-----+------+----+
    * |  depName|empNo|salary|lead|
    * +---------+-----+------+----+
    * |  develop|    8|  6000|5200|
    * |  develop|   10|  5200|4500|
    * |  develop|   11|  5200|4200|
    * |  develop|    9|  4500|null|
    * |  develop|    7|  4200|null|
    * |    sales|    1|  5000|4800|
    * |    sales|    3|  4800|null|
    * |    sales|    4|  4800|null|
    * |personnel|    2|  3900|null|
    * |personnel|    5|  3500|null|
    * +---------+-----+------+----+
    *
    */

  empsalary
    .withColumn("lead", lead($"salary", 2).over(byDeptOrder))
//        .show

  /**
    * Custom window definition
    *
    * - if we would like to change the boundaries of the window
    */

  /**
    * rangeBetween
    *
    * - It can define the boundaries explicitly
    *
    * By default for develop department, start of the window is min value of salary, and end of the window is max value of salary
    *
    * let’s define the start as 100 and end as 300 units from current salary
    *
    * - For depname=develop, salary=4200, start of the window will be (current value + start) which is 4200 + 100 = 4300.
    *   End of the window will be (current value + end) which is 4200 + 300 = 4500
    *
    * - for depname=develop, salary=4500, the window will be (start : 4500 + 100 = 4600, end : 4500 + 300 = 4800).
    *   But there are no salary values in the range 4600 to 4800 inclusive for develop department so max value will be null
    *
    * +---------+-----+------+----------+
    * |  depName|empNo|salary|max_salary|
    * +---------+-----+------+----------+
    * |  develop|    8|  6000|      null|
    * |  develop|   10|  5200|      null|
    * |  develop|   11|  5200|      null|
    * |  develop|    9|  4500|      4200|
    * |  develop|    7|  4200|      null|
    * |    sales|    1|  5000|      4800|
    * |    sales|    3|  4800|      null|
    * |    sales|    4|  4800|      null|
    * |personnel|    2|  3900|      null|
    * |personnel|    5|  3500|      null|
    * +---------+-----+------+----------+
    *
    */


  val winSpec = Window.partitionBy("depName").orderBy("salary")
    .rangeBetween(100L, 300L)

  empsalary
    .withColumn("max_salary", max($"salary").over(winSpec))
//    .show

  /**
    * special boundary values which can be used here.
    * - Window.currentRow: to specify a current value in a row.
    * - Window.unboundedPreceding: This can be used to have an unbounded start for the window.
    * - Window.unboundedFollowing: This can be used to have an unbounded end for the window.
    *
    * Example
    *
    * we need to find the max salary which is greater than 300 from employee salary.
    * So we’ll define the start value as 300L and define the end value as Window.unboundedFollowing
    *
    * - for depname = personnel, salary = 3500. the window will be (start : 3500 + 300 = 3800, end : unbounded).
    *   So the maximum value in this range is 3900 (check output above).
    *
    * - for depname = sales, salary = 4800, the window will be (start : 4800 + 300, 5100, end : unbounded).
    *   Since there are no values greater than 5100 for sales department, null results.
    *
    * +---------+-----+------+----------+
    * |  depName|empNo|salary|max_salary|
    * +---------+-----+------+----------+
    * |  develop|    7|  4200|      6000|
    * |  develop|    9|  4500|      6000|
    * |  develop|   10|  5200|      6000|
    * |  develop|   11|  5200|      6000|
    * |  develop|    8|  6000|      null|
    * |    sales|    3|  4800|      null|
    * |    sales|    4|  4800|      null|
    * |    sales|    1|  5000|      null|
    * |personnel|    5|  3500|      3900|
    * |personnel|    2|  3900|      null|
    * +---------+-----+------+----------+
    *
    */

  val winSpecUnboundFoll = Window.partitionBy("depName").orderBy("salary")
    .rangeBetween(300L, Window.unboundedFollowing)

  empsalary
    .withColumn("max_salary", max($"salary").over(winSpecUnboundFoll))
//    .show

  /**
    *
    * rowsBetween
    *
    * With rangeBetween, we defined the start and end of the window using the value of the ordering column.
    * However, we can also define the start and end of the window with the relative row position.
    *
    * For example, we would like to create a window where start of the window is one row prior to current and end is one row after current row
    *
    * - For depname = develop, salary = 4500, a window will be defined with one row prior and after the current row (highlighted in green).
    *   So salaries within the window are (4200, 4500, 5200) and max is 5200
    *
    * - for depname = sales, salary =5000, a window will be defined with one prior and after the current row.
    *   Since there are no rows after this row, the window will only have 2 rows (highlighted in green) which have salaries as (4800, 5000) and max is 5000
    *
    * +---------+-----+------+----------+
    * |  depName|empNo|salary|max_salary|
    * +---------+-----+------+----------+
    * |  develop|    7|  4200|      4500|
    * |  develop|    9|  4500|      5200|
    * |  develop|   10|  5200|      5200|
    * |  develop|   11|  5200|      6000|
    * |  develop|    8|  6000|      6000|
    * |    sales|    3|  4800|      4800|
    * |    sales|    4|  4800|      5000|
    * |    sales|    1|  5000|      5000|
    * |personnel|    5|  3500|      3900|
    * |personnel|    2|  3900|      3900|
    * +---------+-----+------+----------+
    *
    * We can also use the special boundaries Window.unboundedPreceding, Window.unboundedFollowing, and Window.currentRow as we did previously with rangeBetween.
    *
    * Note: Ordering is not necessary with rowsBetween, but We have used it to keep the results consistent on each run.
    *
    */

  val winSpecRowsBtw = Window.partitionBy("depName")
    .orderBy("salary").rowsBetween(-1, 1)

  empsalary
    .withColumn("max_salary", max($"salary").over(winSpecRowsBtw))
      .show



}

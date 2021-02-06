package atech.guide.concepts

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Joins are Wide Transformations
  * - They are READ Expensive
  * - Spark scans entire Data  frame across entire cluster. So data is going to be moved around in various spark nodes
  */
object Joins extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /*
    * +--------------------+---+------+------------+
    * |          guitarType| id|  make|       model|
    * +--------------------+---+------+------------+
    * |Electric double-n...|  0|Gibson|    EDS-1275|
    * |            Electric|  5|Fender|Stratocaster|
    * |            Electric|  1|Gibson|          SG|
    * |            Acoustic|  2|Taylor|         914|
    * |            Electric|  3|   ESP|        M-II|
    * +--------------------+---+------+------------+
    */
  private val guitarDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  /*
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   0|    [0]|  0|  Jimmy Page|
    * |   1|    [1]|  1| Angus Young|
    * |   2| [1, 5]|  2|Eric Clapton|
    * |   3|    [3]|  3|Kirk Hammett|
    * +----+-------+---+------------+
    */
  private val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  /*
    * +-----------+---+------------+----+
    * |   hometown| id|        name|year|
    * +-----------+---+------------+----+
    * |     Sydney|  1|       AC/DC|1973|
    * |     London|  0|Led Zeppelin|1968|
    * |Los Angeles|  3|   Metallica|1981|
    * |  Liverpool|  4| The Beatles|1960|
    * +-----------+---+------------+----+
    */
  private val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // Joins

  /**
    * INNER JOIN
    */

  /*
    * +----+-------+---+------------+-----------+---+------------+----+
    * |band|guitars| id|        name|   hometown| id|        name|year|
    * +----+-------+---+------------+-----------+---+------------+----+
    * |   1|    [1]|  1| Angus Young|     Sydney|  1|       AC/DC|1973|
    * |   0|    [0]|  0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
    * |   3|    [3]|  3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
    * +----+-------+---+------------+-----------+---+------------+----+
    */

  val joinCondition: Column = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristDF.join(bandsDF, joinCondition, "inner")


  /**
    * OUTER JOIN - LEFT Outer JOIN
    */
  /*
    *
    * Everything in INNER Join + All the rows in the LEFT table, with nulls where the data is missing
    *
    * +----+-------+---+------------+-----------+----+------------+----+
    * |band|guitars| id|        name|   hometown|  id|        name|year|
    * +----+-------+---+------------+-----------+----+------------+----+
    * |   0|    [0]|  0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
    * |   1|    [1]|  1| Angus Young|     Sydney|   1|       AC/DC|1973|
    * |   2| [1, 5]|  2|Eric Clapton|       null|null|        null|null|
    * |   3|    [3]|  3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
    * +----+-------+---+------------+-----------+----+------------+----+
    */
  guitaristDF.join(bandsDF, joinCondition, "left_outer")


  /**
    * OUTER JOIN - RIGHT Outer JOIN
    */
  /*
    *
    * Everything in INNER Join + All the rows in the RIGHT table, with nulls where the data is missing
    *
    * +----+-------+----+------------+-----------+---+------------+----+
    * |band|guitars|  id|        name|   hometown| id|        name|year|
    * +----+-------+----+------------+-----------+---+------------+----+
    * |   1|    [1]|   1| Angus Young|     Sydney|  1|       AC/DC|1973|
    * |   0|    [0]|   0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
    * |   3|    [3]|   3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
    * |null|   null|null|        null|  Liverpool|  4| The Beatles|1960|
    * +----+-------+----+------------+-----------+---+------------+----+
    */
  guitaristDF.join(bandsDF, joinCondition, "right_outer")


  /**
    * OUTER JOIN
    */
  /*
    *
    * Everything in INNER Join + All the rows in BOTH tables, with nulls where the data is missing
    *
    * CAREFUL with outer joins with non-unique keys
    *
    * +----+-------+----+------------+-----------+----+------------+----+
    * |band|guitars|  id|        name|   hometown|  id|        name|year|
    * +----+-------+----+------------+-----------+----+------------+----+
    * |   0|    [0]|   0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
    * |   1|    [1]|   1| Angus Young|     Sydney|   1|       AC/DC|1973|
    * |   2| [1, 5]|   2|Eric Clapton|       null|null|        null|null|
    * |   3|    [3]|   3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
    * |null|   null|null|        null|  Liverpool|   4| The Beatles|1960|
    * +----+-------+----+------------+-----------+----+------------+----+
    *
    */
  guitaristDF.join(bandsDF, joinCondition, "outer")

  /**
    * SEMI JOIN
    */

  /*
    * Everything in the left DF for which there is a row in the right DF satisfying the condition
    * Everything in INNER Join - Data from RIGHT Dataframe
    *
    * Essentially a filter
    *
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   0|    [0]|  0|  Jimmy Page|
    * |   1|    [1]|  1| Angus Young|
    * |   3|    [3]|  3|Kirk Hammett|
    * +----+-------+---+------------+
    */
  guitaristDF.join(bandsDF, joinCondition, "left_semi")

  /**
    * ANTI JOIN
    */
  /*
    * Everything in the left DF for which there is NO row in the right DF satisfying the condition
    * Keeps the data in Left Dataframe for which there is no row in Right dataframe satisfying the join condition
    *
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   2| [1, 5]|  2|Eric Clapton|
    * +----+-------+---+------------+
    *
    */
  guitaristDF.join(bandsDF, joinCondition, "left_anti")


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes as we have two id's in resulting dataframe

  // Option 1 - Rename the column on which we are joining
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // Option 2 - Drop the dupe column
  // This works because spark maintains a unique identifier for all the columns it operates on.
  // We are giving spark unique identifier of a column to drop
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // Option 3 - Rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandID")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandID"))

  /**
    * Using Complex Types
    *
    * +----+-------+---+------------+--------------------+--------+------+------------+
    * |band|guitars| id|        name|          guitarType|guitarID|  make|       model|
    * +----+-------+---+------------+--------------------+--------+------+------------+
    * |   0|    [0]|  0|  Jimmy Page|Electric double-n...|       0|Gibson|    EDS-1275|
    * |   2| [1, 5]|  2|Eric Clapton|            Electric|       5|Fender|Stratocaster|
    * |   1|    [1]|  1| Angus Young|            Electric|       1|Gibson|          SG|
    * |   2| [1, 5]|  2|Eric Clapton|            Electric|       1|Gibson|          SG|
    * |   3|    [3]|  3|Kirk Hammett|            Electric|       3|   ESP|        M-II|
    * +----+-------+---+------------+--------------------+--------+------+------------+
    */
  guitaristDF.join(guitarDF.withColumnRenamed("id", "guitarID"), expr("array_contains(guitars, guitarId)"))

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  def readTable(table: String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/atechguide")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$table")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNo = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalaryDF = employeesDF.join(maxSalariesPerEmpNo, "emp_no")

  // 2 [Cross Verify query -> select * from dept_manager where emp_no=12940;]
  val employeeNeverManagerDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )
  // 3
  val mostRecentJobTitles = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalaryDF
    .orderBy(col("maxSalary").desc)
    .limit(10)

  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitles, "emp_no")
  bestPaidJobsDF.show()



}

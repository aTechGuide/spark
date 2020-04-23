package atech.guide.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") //<- We need this for `.mode(SaveMode.Overwrite)` to work as expected while writing tables
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Regular DF API
  carsDF.select(col("Name"))
    .where(col("Origin") === "USA")

  /**
    * Spark SQL API
    */

  // Creating an Alias in Spark so that it can refer to it as an table
  carsDF.createOrReplaceTempView("cars")
  val americalCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin)

  // we can run any SQL Statements
  spark.sql("create database atechguide")
  spark.sql("use atechguide")
  val databasesDF = spark.sql("show databases")
  // databasesDF.show()

  // transfer table from a DB to Spark tables

  def readTable(table: String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/atechguide")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$table")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false): Unit =
    tableNames.foreach { tableName =>
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)

      if (shouldWriteToWarehouse) {
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
    }

   transferTables(List("employees", "departments", "dept_manager", "dept_emp", "salaries"))

  // Read DF from loaded spark tables
//  val employeesDF2 = spark.read
//    .table("employees")

  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as a Spark table in the atechguide database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  // 2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
    """.stripMargin
  )

  // 3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group by de.dept_no
    """.stripMargin
  )

  // 4
  spark.sql(
    """
      |select avg(s.salary) as payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
    """.stripMargin
  ).show()

}

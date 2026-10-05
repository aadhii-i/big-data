import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lab10 Task1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val filePath = "employee.csv"

    // a. Load employee dataset from CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("A. SCHEMA AND FIRST 10 RECORDS")
    println("==============================================")

    df.printSchema()
    df.show(10, false)


    // b. Display only employees belonging to Sales department
    val salesEmployees = df.filter($"Department" === "Sales")

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("B. SALES DEPARTMENT EMPLOYEES")
    println("==============================================")

    salesEmployees.show(false)


    // c. Employees whose salary is greater than 50000
    val highSalaryEmployees = df.filter($"Salary" > 50000)

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("C. SALARY GREATER THAN 50000")
    println("==============================================")

    highSalaryEmployees.show(false)


    // d. Display only Name, Department and Salary
    val selectedColumns = df.select(
      $"Name",
      $"Department",
      $"Salary"
    )

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("D. NAME, DEPARTMENT AND SALARY")
    println("==============================================")

    selectedColumns.show(false)


    // e. Calculate average salary for each department
    val averageSalary = df
      .groupBy($"Department")
      .agg(
        avg($"Salary").alias("AverageSalary")
      )

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("E. AVERAGE SALARY BY DEPARTMENT")
    println("==============================================")

    averageSalary.show(false)


    // f. Maximum salary in each department
    val maximumSalary = df
      .groupBy($"Department")
      .agg(
        max($"Salary").alias("MaximumSalary")
      )

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("F. MAXIMUM SALARY BY DEPARTMENT")
    println("==============================================")

    maximumSalary.show(false)


    // g. Number of employees in each department
    val employeeCount = df
      .groupBy($"Department")
      .count()

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("G. EMPLOYEE COUNT BY DEPARTMENT")
    println("==============================================")

    employeeCount.show(false)


    // h. Sort employees by salary descending and display top 5
    val top5Employees = df
      .orderBy(desc("Salary"))
      .limit(5)

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("H. TOP 5 EMPLOYEES BY SALARY")
    println("==============================================")

    top5Employees.show(false)


    // i. Create SalaryCategory column
    val categorizedDF = df.withColumn(
      "SalaryCategory",
      when($"Salary" > 80000, "High")
        .when($"Salary" >= 50000, "Medium")
        .otherwise("Low")
    )

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("I. SALARY CATEGORY")
    println("==============================================")

    categorizedDF.show(false)


    // j. Register DataFrame as temporary Spark SQL view
    categorizedDF.createOrReplaceTempView("employees")

    val experiencedEmployees = spark.sql("""
      SELECT *
      FROM employees
      WHERE Experience > 5
    """)

    println("\n==============================================")
    println("Name: Adhil Rahiman M")
    println("Roll No: 2023BCS0187")
    println("J. EMPLOYEES WITH MORE THAN 5 YEARS EXPERIENCE")
    println("==============================================")

    experiencedEmployees.show(false)


    spark.stop()
  }
}

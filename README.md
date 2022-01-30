# Spark-Scala - Quick Reference Guide

### Spark-Scala : DataFrame : Option # 1

```
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Department(deptId: Integer, deptName: String)
val department1 = Department(101, "a1")
val department2 = Department(102, "a2")
val df1: DataFrame = Seq(department1, department2).toDF
```

### Spark-Scala : DataFrame : Option # 2

```
import org.apache.spark.sql.{DataFrame, SparkSession}
val spark = SparkSession.builder().appName("SparkScapaApp").master("local[*]").getOrCreate()

val mockedData = List(
  (10001, 1, "Samsung", "Galaxy10"),
  (10002, 1, "Samsung", "Galaxy20"),
)
import spark.implicits._
val df = spark.sparkContext.parallelize(mockedData).toDF("accountNbr", "manufacturer_name", "model_name")
```

### Spark-Scala : DataFrame : Option # 3
```
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

val testDataList = List(
  Row(1001, "a1"),
  Row(1002, "a2")
)

val testDataSchema = StructType(
  List(
    StructField("dept_id", IntegerType, false),
    StructField("dept_name", StringType, false)
  )
)

val df = spark.createDataFrame(spark.sparkContext.parallelize(testDataList),testDataSchema)
```

### Spark-Scala : DataFrame : Write to Table # Option # 1
```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}


//
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()

// todo - build dataframe from above examples
val dataFrame = spark.emptyDataFrame

//
insert2Account(spark, dataFrame)


def insert2Account(spark: SparkSession, df: DataFrame): Unit = {
  //
  val listCols =
    List(
      "account_id",
      "first_name",
      "last_name",
      "sys_creation_date"
    )
  val accountDataFrame = df.select(listCols.map(m => col(m)): _*)

  //
  val tableName: String = "account"
  
  //
  accountDataFrame.write
                  .format("delta")
                  .mode("overwrite")
                  .saveAsTable(tableName)
}
```

### Spark-Scala : DataFrame : Write to Table # Option # 2
```
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}


//
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()

// todo - build dataframe from above examples
val dataFrame = spark.emptyDataFrame

//
insert2Account(spark, dataFrame)


def insert2Account(spark: SparkSession, df: DataFrame): Unit = {
    //
    val accountDataFrame = df
      .withColumn("created_date", current_timestamp())
      .withColumn("updated_date", current_timestamp())

    //
    val upsertCondition = "target.account_id = source.account_id"

    //
    val tableName: String = "account"
    val deltaTable = DeltaTable.forName(spark, tableName)
    deltaTable
      .as("target")
      .merge(accountDataFrame.alias("source"), upsertCondition)
      .whenMatched
      .updateExpr(
        Map(
          "updated_date" -> "source.exp_ev_tmstmp",
          "first_name" -> "source.first_name",
          "last_name" -> "source.last_name"
        )
      )
      .whenNotMatched
      .insertAll()
      .execute()
  }

```

### Spark-Scala : DataFrame : extract single column data to list : Option # 1 

```
 //
val spark = SparkSession
  .builder()
  .appName("SparkScalaApp")
  .master("local[*]")
  .getOrCreate()

// todo - build dataframe from above examples
val dataFrame = spark.emptyDataFrame

val empIdList = dataFrame.select("emp_id").distinct.collect.flatMap(_.toSeq)
```

### Spark-Scala : DataFrame : extract single column data to list : Option # 2

```
//
val spark = SparkSession
  .builder()
  .appName("SparkScalaApp")
  .master("local[*]")
  .getOrCreate()

// todo - build dataframe from above examples
val dataFrame = spark.emptyDataFrame

val empIdList = dataFrame.select("id").collect().map(_(0)).toList
```

### Spark-Scala : Oracle CRUD : extract single column data to list
```
object DBUtil {
    /**
     * this code is not tested properly
     * 1. verify the db col names & dataframe are same.
     * 2. override - will remove all records from db and will insert
     * 3. append  - not tested
     */
    
    def insertRecords(df: DataFrame, tableName: String): Unit = {
      val jdbcPort = 1234;
      val jdbcUsername = "abcd"
      val jdbcHost = "127.0.01";
      val jdbcPassword = "abcxyz"
      val jdbcSidSchema = "sid-schema"
      val driverDriver = "oracle.jdbc.OracleDriver"
      val jdbcUrl = s"jdbc:oracle:thin:@$jdbcHost:$jdbcPort:$jdbcSidSchema"
      println(s"jdbcUrl => $jdbcUrl")

      df.write
        .format("jdbc")
        .options(
          Map(
            "url" -> jdbcUrl,
            "user" -> jdbcUsername,
            "password" -> jdbcPassword,
            "dbtable" -> tableName,
            "driver" -> driverDriver
          )
        )
        .mode(SaveMode.Overwrite)
        .save()
    }

    def selectRecords(spark: SparkSession): Unit = {
      val selectQry =
        "(select count(1) as total_count from emp) tab"
      println(s"Qry => $selectQry")

      val jdbcPort = 1234;
      val jdbcUsername = "abcd"
      val jdbcHost = "127.0.01";
      val jdbcPassword = "abcxyz"
      val jdbcSidSchema = "sid-schema"
      val driverDriver = "oracle.jdbc.OracleDriver"
      val jdbcUrl = s"jdbc:oracle:thin:@$jdbcHost:$jdbcPort:$jdbcSidSchema"
      println(s"jdbcUrl => $jdbcUrl")

      val jdbcDataFrame = spark.read
        .format("jdbc")
        .options(
          Map(
            "url" -> jdbcUrl,
            "user" -> jdbcUsername,
            "password" -> jdbcPassword,
            "dbtable" -> selectQry,
            "driver" -> driverDriver
          )
        )
        .load()

      if (jdbcDataFrame.count() != 0) {
        val totalCount = jdbcDataFrame.select("total_count")
        println(s"totalCount = $totalCount")
      }
    }

    def deleteRecords(): Unit = {
      val jdbcPort = 1234;
      val jdbcUsername = "abcd"
      val jdbcHost = "127.0.01";
      val jdbcPassword = "abcxyz"
      val jdbcSidSchema = "sid-schema"
      val driverDriver = "oracle.jdbc.OracleDriver"
      val jdbcUrl = s"jdbc:oracle:thin:@$jdbcHost:$jdbcPort:$jdbcSidSchema"
      println(s"jdbcUrl => $jdbcUrl")

      var connection: Connection = null
      var deletedRowsCount: Int = 0
      try {
        Class.forName(driverDriver);
        connection =
          DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);

        val deleteQry = "DELETE FROM xyz"
        val prepareStatement: PreparedStatement =
          connection.prepareStatement(deleteQry)
        try {
          deletedRowsCount = prepareStatement.executeUpdate();
        } finally {
          prepareStatement.close();
        }
      } catch {
        case e: SQLException => e.printStackTrace();
      } finally {
        connection.close();
      }
    }
  }
```

### Spark-Scala : Split DataFrame based on empId

```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

val spark = SparkSession.builder()
                        .appName("SparkScalaApp")
                        .master("local[*]")
                        .getOrCreate()

val data = List(
      (101, "2022-01-16", "a1"),
      (101, "2022-01-17", "b1"),
      (101, "2022-01-18", "c1"),
      (101, "2022-01-19", "a1"),
      (101, "2022-01-20", "b1"),
      //
      (102, "2022-01-25", "a1"),
      (102, "2022-01-26", "b1"),
      //
      (103, "2022-01-11", "a1"),
      (103, "2022-01-12", "a1"),
      (103, "2022-01-13", "a1"),
      (103, "2022-01-14", "b1"),
      (103, "2022-01-15", "a1"),
      (103, "2022-01-16", "a1"),
      (103, "2022-01-26", "b1")
    )

    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(data)
      .toDF(
        "emp_id",
        "creation_date",
        "name"
      );

    val empIdList = df.select("emp_id").distinct.collect.flatMap(_.toSeq)
    empIdList.foreach(println)

    val dataFrameList =
      empIdList.map(empData => df.where($"emp_id" <=> empData))

    val columnNames = Seq(
      "emp_id",
      "creation_date",
      "name"
    )

    dataFrameList.foreach(dataFrame => {
      val smDataFrame = getTopRecordsDataFrame(
        dataFrame,
        columnNames,
        filterColName = "name",
        filterColValue = "b1",
        orderedByColName = "creation_date",
        limitNbr = 3
      )

      printRecordData(spark, smDataFrame)
    })
  }

  def printRecordData(spark: SparkSession, df: DataFrame): Unit = {
    //
    df.createOrReplaceTempView("temp_data_view")
    //
    val qry = "select * from temp_data_view";
    val qryDataFrame = spark.sql(qry)
    val sqlQryDataList = qryDataFrame.collect()
    sqlQryDataList.foreach { rowData =>
      val sysCreationDate: String = rowData.getAs[String]("creation_date")
      val name: String = rowData.getAs[String]("name")
      val empId: Int = rowData.getAs[Int]("emp_id")

      println(
        s"empId => $empId, name => $name, sysCreationDate => $sysCreationDate"
      )
    }
  }

  def getTopRecordsDataFrame(
      df: Dataset[Row],
      dataFrameAllColNames: Seq[String],
      filterColName: String,
      filterColValue: String,
      orderedByColName: String,
      limitNbr: Int
  ): Dataset[Row] = {
    val filteredDataFrame = df
      .select(dataFrameAllColNames.map(name => col(name)): _*)
      .filter(df(filterColName) === filterColValue)
      .orderBy(col(orderedByColName).desc)
      .limit(limitNbr)
    filteredDataFrame
  }

```

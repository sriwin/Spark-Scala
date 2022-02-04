# Spark-Scala - Quick Reference Guide

### Scala : Null, null, Nil, Nothing, None, and Unit
* null - Similar to Java null, void using null while initializing variables, use Nil if you want to set an empty value to the variable

```
java   => private String nullString = null;
Scala => val nullString: String = null
```

* Null - TODO
```
```

* Nothing - TODO
```
```

* Nil - Empty List
```
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

```
val intList01: List[Int] = Nil
val intList02: List[Int] = 1 :: Nil
print("intList01 = " + intList01.size + " && intList02 = " + intList02.size)


val testDataSchema = StructType(
      StructField("dept_id", IntegerType, false) ::
      StructField("dept_name", StringType, false) :: Nil
    )
```

* None -  Used to return a valid by avoiding null pointer exception. Option has exactly 2 subclasses i.e. None & Some

```
val someOptionInt: Option[Int] = Some(111)
val noneOptionInt: Option[Int] = None
```

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
import org.apache.spark.sql.{SparkSession}
//
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()

val data = List(
      ("f1", "l1", "M", 10000),
      ("f2", "l2", "F", 20000),
      ("f3", "l3", "M", 30000)
)

val cols =Seq("first_name", "last_name", "gender", "salary")
val df = spark.createDataFrame(data).toDF(cols: _*)
```

### Spark-Scala : DataFrame : Option # 4
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

### Spark-Scala : Create DataFrame using Map

```
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()

val map = Map("a1" -> "1", "a2" -> "2", "a3" -> "3")
import spark.implicits._
val df = map.toSeq.toDF("col", "data")
df.show(false)
```


### Spark-Scala : Create DataFrame using List

```
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()

val list = List(101, 102, 103)
val t: List[(Int, String)] = list map ((_, "0"))
import spark.implicits._
val df1 = t.toSeq.toDF("col", "data")
df1.show(false)    
```

### Spark-Scala : List 2 Map # 01

```
val list01 = List(101L, 102L, 103L)
val list02 = List("a1", "a2", "a3")
val map01 = (list01 zip list02).toMap
map01.foreach(println)
```

### Spark-Scala : List 2 Map # 02

```
val map02 = list01.zipWithIndex.map { case (v, i) => (v, i) }.toMap
map02.foreach(println)
```

### Spark-Scala : List 2 Map # 03

```
 val list03 = List(101, 102, 103)
    val t: List[(Int, String)] = list03 map ((_, "0"))
    t.foreach(println)
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

### Spark-Scala : DataFrame : Write to Table with Timestamp 
```
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{
  col,
  current_timestamp,
  lit,
  unix_timestamp
}
import org.apache.spark.sql.types.{
  DataType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.{Date, Timestamp}


//
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()

import spark.implicits._
val insertDataList = Seq(
	Row(101L, Timestamp.valueOf("1980-01-01 09:01:01"), "M", "a1")
)

val insertDataSchema = StructType(
      List(
        StructField("emp_id", LongType, false),
        StructField("dob", TimestampType, false),
        StructField("gender", StringType, false),
        StructField("name", StringType, false),
        StructField("is_active", StringType, false),
      )
    )

val df = spark.createDataFrame(spark.sparkContext.parallelize(insertDataList),insertDataSchema)
              .withColumn("CREATED_TIMESTAMP", current_timestamp())
              .withColumn("UPDATED_TIMESTAMP", current_timestamp())
              .withColumn("is_active", lit("true"))
              
val df1 = df.select(col("emp_id"),
					col("dob").cast(TimestampType),
					col("gender"),
					col("name"),
					col("CREATED_TIMESTAMP"),
					col("UPDATED_TIMESTAMP"),
                                        col("is_active")
				   )

val df2 = df1.withColumn("dob", unix_timestamp(col("dob"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
			 .withColumn("CREATED_TIMESTAMP",unix_timestamp(col("CREATED_TIMESTAMP"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
			.withColumn("UPDATED_TIMESTAMP",unix_timestamp(col("MODIFIED_TIMESTAMP"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

val deltaLakeTableName = "emp"
val upsertCondition = "target.emp_id = source.emp_id"

val deltaTable = DeltaTable.forName(spark, deltaLakeTableName)
deltaTable.as("target")
          .merge(df2.alias("source"), upsertCondition)
          .whenMatched
          .updateExpr(Map("dob" -> "source.dob",
					"gender" -> "source.gender",
                                        "is_active" -> "source.is_active",
					"name" -> "source.name",
					"CREATED_TIMESTAMP" -> "source.CREATED_TIMESTAMP",
					"UPDATED_TIMESTAMP" -> "source.MODIFIED_TIMESTAMP"))
		  .whenNotMatched
		  .insertAll()
		  .execute()
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

### Spark-Scala - DataFrame - Cast Column to another Type

```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

def castColumnTo(dataFrame: DataFrame, colName: String, castType: DataType ) : DataFrame = {
    dataFrame.withColumn(colName, dataFrame(colName).cast(castType) )
}
```

### Spark-Scala - Cast List[Any] to List[Long]
```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val empIdList = df.select("emp_id").collect().map(_(0)).toList.map(_.toString.toLong)
```

### Spark-Scala - DataFrame : Cast Int to Long
```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val df1 = df.withColumn("emp_id", col("emp_id").cast(LongType))
```

### Spark-Scala - Filter DataFrame using List
```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val qryDataFrame = spark.sql(qry).filter(col("emp_id").isin(empList: _*))
```

### Spark-Scala - Filter DataFrame using user variable
```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val sqlQryDataFrame = spark.sql(sqlQry)
sqlQryDataFrame.filter(sqlQryDataFrame("execution_nbr") === executionNbr)
```

### Spark-Scala - Read CSV from Databricks FileStore Location
```
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val testDataFileLocation = "/FileStore/tables/test1.csv"
val fileType = "csv"

//# CSV options
val inferSchema = "true"
val isFirstRowHeader = "true"
val delimiter = ","

//
val deltaLakeTableName: String = "qa_test_data"

//
val df = spark.read
              .format(fileType)
              .option("inferSchema", inferSchema)
              .option("header", isFirstRowHeader)
              .option("sep", delimiter)
              .load(testDataFileLocation)
//
testDataDataFrame.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(deltaLakeTableName)
```

### Spark-Scala : Convert Timestamp Column to String
```
import org.apache.spark.sql.functions.{col, date_format, to_timestamp}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//
val spark = SparkSession.builder()
                        .appName("SparkScalaApp")
                        .master("local[*]")
                        .getOrCreate()

import spark.implicits._
val timestamp2StringDataFrame = Seq(
  (Timestamp.valueOf("2022-01-29 06:00:01"))
).toDF("created_timestamp")

val stringDataFrame = castTimestamp2String(timestamp2StringDataFrame,
"created_timestamp", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd")
println("########################################")
println("#### Timestamp 2 String - DataFrame ")
println("########################################")
stringDataFrame.show(false)

def castTimestamp2String(dataFrame: DataFrame, colName: String, inputFormat: String, outputFormat: String): DataFrame = {
    dataFrame.withColumn(colName,date_format(to_timestamp(col(colName), inputFormat), outputFormat))
}
```

### Spark-Scala : Convert String Column to Timestamp
```
import org.apache.spark.sql.functions.{col, date_format, to_timestamp}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//
val spark = SparkSession.builder()
                        .appName("SparkScalaApp")
                        .master("local[*]")
                        .getOrCreate()
                        
val string2TimestampDataFrame = Seq(
      ("2022-01-29 06:00:01")
    ).toDF("created_timestamp")

val timestampDataFrame = CastUtil.castString2Timestamp(string2TimestampDataFrame, "created_timestamp", 
"yyyy-MM-dd HH:mm:ss")

println("########################################")
println("#### String 2 Timestamp - DataFrame ")
println("########################################")
timestampDataFrame.show(false)
```

### Spark-Scala : DataFrame : Add New Column using withColumn
```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

//
val spark = SparkSession
  .builder()
  .appName("SparkScalaApp")
  .master("local[*]")
  .getOrCreate()

val data = List(
  ("f1", "l1", "M", 10000),
  ("f2", "l2", "F", 20000),
  ("f3", "l3", "M", 30000)
)

val cols =
  Seq("first_name", "last_name", "gender", "salary")
val df = spark.createDataFrame(data).toDF(cols: _*)

var df1 = df
      .withColumn("phone", lit("null").as("StringType"))
      .withColumn("po_order", lit(0.0).as("DoubleType"))
      .withColumn("created_date", current_timestamp())
      .withColumn("updated_date", current_timestamp())
    df1.show(false)

df1.show(false)
```


### Spark-Scala : DataFrame : Add New Column using withColumn and when Condition

```
Syntax - 
df.withColumn("new_column_name",  when(<column_condition>, <value_when_true>).otherwise(<value_when_false>))

Example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}


//
val spark = SparkSession
  .builder()
  .appName("SparkScalaApp")
  .master("local[*]")
  .getOrCreate()

val data = List(
  ("f1", "l1", "M", 10000, "galaxy4", 25),
  ("f2", "l2", "F", 20000, "galaxy6", 35),
  ("f3", "l3", "M", 30000, "galaxy10", 45),
  ("f4", "l4", "N", 40000, "galaxy20", 55)
)

val cols =
  Seq("first_name", "last_name", "gender", "salary", "phone_model", "age")
val df = spark.createDataFrame(data).toDF(cols: _*)

var df1 = df
  .withColumn("phone", lit("null").as("StringType"))
  .withColumn("po_order", lit(0.0).as("DoubleType"))
  .withColumn("created_date", current_timestamp())
  .withColumn("updated_date", current_timestamp())
  .withColumn(
	"sal_gt_1000",
	when(col("salary") > 1000, true).otherwise(false)
  )
  .withColumn(
	"new_gender",
	when(col("gender").equalTo("N"), "NA")
  )
  .withColumn(
	"phone_make_year",
	when(col("phone_model") === "galaxy4", 2008)
	  .when(col("phone_model").isin("galaxy6", "galaxy10"), 2010)
	  .when(col("phone_model").isin("galaxy20"), 2020)
	  .otherwise(2005)
  )
df1.show(false)
```

### Spark-Scala : DataFrame : Add New Column using withColumn and when Condition
```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

//
val spark = SparkSession
  .builder()
  .appName("SparkScalaApp")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._
val df = Seq(
  (Date.valueOf("2019-01-01"), "n", 200.00),
  (Date.valueOf("2019-05-10"), "n", 400.00),
  (Date.valueOf("2019-03-05"), "s", 100.00),
  (Date.valueOf("2019-02-20"), "c", 500.00),
  (Date.valueOf("2019-01-20"), "s", 300.00),
  (Date.valueOf("2019-02-15"), "l", 700.00),
  (Date.valueOf("2019-07-01"), "c", 700.00),
  (Date.valueOf("2019-04-01"), "s", 400.00)
).toDF("create_date", "category", "price")

val df1 = df
  .withColumn(
	"row_nbr",
	row_number() over Window.partitionBy("category").orderBy("create_date")
  )

df1.show(false)
```

### Spark-Scala - DataFrame :  when Condition & filter conditions
```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

//
val spark = SparkSession
  .builder()
  .appName("SparkScalaApp")
  .master("local[*]")
  .getOrCreate()

import spark.sqlContext.implicits._
val data = List(
  ("f1", "l1", "M", 25),
  ("f2", "l2", "F", 35),
  ("f3", "l3", "M", 45),
  ("f4", "l4", "N", 55)
)

val cols =
  Seq("first_name", "last_name", "gender", "age")
val df = spark.createDataFrame(data).toDF(cols: _*)

val df2 = df
  .withColumn(
	"new_gender_01",
	when(col("gender") === "M", "Male")
	  .when(col("gender") === "F", "Female")
	  .otherwise("Unknown")
  )
  .withColumn(
	"new_gender_02",
	when(col("gender") === "M" && col("gender") === "F", "Male/Female")
	  .otherwise("Unknown")
  )
  .withColumn(
	"new_gender_03",
	when(col("gender") === "M" || col("gender") === "F", "Male/Female")
	  .otherwise("Unknown")
  )
df2.show(false)

val df3 = df2.filter(df("age") > 40)
df3.show(false)
```

### Spark-Scala - remove temp view tables
```
spark.catalog.dropTempView("tab_data_view")
```

### Databricks - remove file from dbfs location
```
dbutils.fs.rm("/FileStore/tables/sample_data.csv")
```

### Spark-Scala : Delete records Using List
```
import org.apache.spark.sql.SparkSession

//
val spark = SparkSession.builder()
						.appName("SparkScalaApp")
						.master("local[*]")
						.getOrCreate()
val tableName = "emp"
val empIdList = List(101, 102)
val deleteQry = s"select * from $tableName where emp_id in (${empIdList.map(x => "'" + x + "'").mkString(",")})"
spark.sql(deleteQry)
```

### Spark-Scala : Select records Using List
```
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("SparkScalaApp").master("local[*]").getOrCreate()
val resultsTableName = "emp"
val empIdList = List(101, 102)
val selectQry = s"select * from $resultsTableName where emp_id in (${empIdList.map(x => "'" + x + "'").mkString(",")})"
val selectQryDataFrame = spark.sql(selectQry)
selectQryDataFrame.show(false)
```


### Spark-Scala : Optimize Table
```
def optimizeTables(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkScalaApp")
      .master("local[*]")
      .getOrCreate()
    val optimizeTableList =
      Array("OPTIMIZE emp_id  ZORDER BY empId", "OPTIMIZE dept")
    optimizeTableList.foreach { item =>
      spark.sql(item)
    }
  }
```


### Spark-Scala : Utility Methods
```
def leftPad(string: String, len: Int, padChar: Char): String = {
    if (string.length >= len) return string
    val stringBuilder = new StringBuilder(len)
    for (i <- string.length until len) {
      stringBuilder.append(padChar)
    }
    stringBuilder.append(string)
    stringBuilder.toString
  }

  def rightPad(string: String, len: Int, padChar: Char): String = {
    if (string.length >= len) return string
    val stringBuilder = new StringBuilder(len)
    stringBuilder.append(string)
    for (i <- string.length until len) {
      stringBuilder.append(padChar)
    }
    stringBuilder.toString
  }

  def getIntList(dbData: String, maxRows: Int): String = {
    //
    val fieldDataList: ListBuffer[Int] = ListBuffer.fill(maxRows)(0)
    val dbDataList = dbData.split(",").toList

    for (i <- 0 until dbDataList.size) {
      fieldDataList(i) = BigDecimal(dbDataList(i)).toInt
    }
    fieldDataList.mkString(",")
  }

  def getStringList(dbData: String, maxRows: Int): String = {
    //
    val fieldDataList: ListBuffer[String] = ListBuffer.fill(maxRows)("")
    val dbDataList = dbData.split(",").toList

    for (i <- 0 until dbDataList.size) {
      fieldDataList(i) = "\"" + dbDataList(i) + "\""
    }
    fieldDataList.mkString(",")
  }

  def addDoubleQuote2ListData(fieldDataList: List[String]): String = {
    var finalData = ""
    //val fieldDefaultValue = "\"\""
    var listStringBuffer = new ListBuffer[String]()
    for (i <- 0 until fieldDataList.size) {
      listStringBuffer += "\"" + fieldDataList(i) + "\""
    }
    finalData = listStringBuffer.mkString(",")
    "[" + finalData + "]"
  }

  def isAllDigits(input: String) = input forall Character.isDigit
```

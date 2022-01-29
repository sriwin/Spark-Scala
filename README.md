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
  val subscriberDataFrame = df.select(listCols.map(m => col(m)): _*)

  //
  val tableName: String = "account"
  
  //
  subscriberDataFrame.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(tableName)
}
```



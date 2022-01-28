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
```


## Spark SQL Examples

```
package com.spark.sql.dataframes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQLExamples extends App {
  val spark = SparkSession.builder().appName("Data Frames").master("local").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext
  val emp = sc.parallelize(Array(("naga", 30, 10), ("ravi", 32, 20), ("hari", 24, 10), ("siva", 26, 30),
    ("rajesh", 28, 30), ("ashok", 33, 40))).toDF("name", "age", "dno")
  emp.createTempView("employee")
  val dept = sc.parallelize(Array((10, "Engineering"), (30, "Sales"), (40, "HR"), (50, "Finance"))).toDF("dno", "dname")
  dept.createTempView("dept")

  spark.sql("select * from employee").show()
  spark.sql("select * from dept").show()
  /*
   * Joins using Spark SQL
   */
  println("### Equi Join ###")
  spark.sql("select * from employee inner join dept on employee.dno = dept.dno").show()
  println("### Left Outer Join ###")
  spark.sql("select * from employee left outer join dept on employee.dno = dept.dno").show()
  println("### Right Outer Join ###")
  spark.sql("select * from employee right outer join dept on employee.dno = dept.dno").show(2)
  println("### Full Outer Join ###")
  spark.sql("select * from employee full outer join dept on employee.dno = dept.dno").show()
  println("### Self Join ###")
  spark.sql("select * from employee E1, employee E2 where E1.dno = E2.dno").show()
}
```

## Results:

```
# Employee Table
+------+---+---+
|  name|age|dno|
+------+---+---+
|  naga| 30| 10|
|  ravi| 32| 20|
|  hari| 24| 10|
|  siva| 26| 30|
|rajesh| 28| 30|
| ashok| 33| 40|
+------+---+---+
# Dept Table
+---+-----------+
|dno|      dname|
+---+-----------+
| 10|Engineering|
| 30|      Sales|
| 40|         HR|
| 50|    Finance|
+---+-----------+

### Equi Join ###
+------+---+---+---+-----------+
|  name|age|dno|dno|      dname|
+------+---+---+---+-----------+
| ashok| 33| 40| 40|         HR|
|  naga| 30| 10| 10|Engineering|
|  hari| 24| 10| 10|Engineering|
|  siva| 26| 30| 30|      Sales|
|rajesh| 28| 30| 30|      Sales|
+------+---+---+---+-----------+

### Left Outer Join ###
+------+---+---+----+-----------+
|  name|age|dno| dno|      dname|
+------+---+---+----+-----------+
|  ravi| 32| 20|null|       null|
| ashok| 33| 40|  40|         HR|
|  naga| 30| 10|  10|Engineering|
|  hari| 24| 10|  10|Engineering|
|  siva| 26| 30|  30|      Sales|
|rajesh| 28| 30|  30|      Sales|
+------+---+---+----+-----------+

### Right Outer Join ###
+-----+---+---+---+-----------+
| name|age|dno|dno|      dname|
+-----+---+---+---+-----------+
|ashok| 33| 40| 40|         HR|
| naga| 30| 10| 10|Engineering|
+-----+---+---+---+-----------+
only showing top 2 rows

### Full Outer Join ###
+------+----+----+----+-----------+
|  name| age| dno| dno|      dname|
+------+----+----+----+-----------+
|  ravi|  32|  20|null|       null|
| ashok|  33|  40|  40|         HR|
|  naga|  30|  10|  10|Engineering|
|  hari|  24|  10|  10|Engineering|
|  null|null|null|  50|    Finance|
|  siva|  26|  30|  30|      Sales|
|rajesh|  28|  30|  30|      Sales|
+------+----+----+----+-----------+

### Self Join ###
+------+---+---+------+---+---+
|  name|age|dno|  name|age|dno|
+------+---+---+------+---+---+
|  ravi| 32| 20|  ravi| 32| 20|
| ashok| 33| 40| ashok| 33| 40|
|  naga| 30| 10|  naga| 30| 10|
|  naga| 30| 10|  hari| 24| 10|
|  hari| 24| 10|  naga| 30| 10|
|  hari| 24| 10|  hari| 24| 10|
|  siva| 26| 30|  siva| 26| 30|
|  siva| 26| 30|rajesh| 28| 30|
|rajesh| 28| 30|  siva| 26| 30|
|rajesh| 28| 30|rajesh| 28| 30|
+------+---+---+------+---+---+

```

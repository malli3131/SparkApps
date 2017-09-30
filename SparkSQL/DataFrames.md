## Spark SQL Data Frames Examples

#### Sample Code in Scala:
```
package com.spark.sql.dataframes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExamples extends App {
  val spark = SparkSession.builder().appName("Data Frames").master("local").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext
  val emp = sc.parallelize(Array(("naga", 30, 10), ("ravi", 32, 20), ("hari", 24, 10), ("siva", 26, 30),
    ("rajesh", 28, 30), ("ashok", 33, 40))).toDF("name", "age", "dno")
  emp.show()
  val dept = sc.parallelize(Array((10, "Engineering"), (30, "Sales"), (40, "HR"), (50, "Finance"))).toDF("dno", "dname")
  dept.show()

  val emp1 = sc.parallelize(Array(("naga", 30, 10), ("ravi", 32, 20), ("hari", 24, 10), ("siva", 26, 30),
    ("rajesh", 28, 30), ("ashok", 33, 40))).toDF("name", "age", "dno")
  emp1.show()
  val dept1 = sc.parallelize(Array((10, "Engineering"), (30, "Sales"), (40, "HR"), (50, "Finance"))).toDF("deptno", "dname")
  dept1.show()
  /*
   * Joins using Data Frames
   */
  println("### Equi Join ###")
  emp1.join(dept1, $"dno" === $"deptno", "inner").show()
  println("### Equi Join ###")
  emp.join(dept, Seq("dno"), "inner").show()
  println("### Left Outer Join ###")
  emp.join(dept, Seq("dno"), "left_outer").show()
  println("### Right Outer Join ###")
  emp.join(dept, Seq("dno"), "right_outer").show()
  println("### Full Outer Join ###")
  emp.join(dept, Seq("dno"), "outer").show()
  println("### Self Join ###")
  emp.join(emp, "dno").show()
}
```
### Results:
```
#1. emp:
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
#2. dept:
+---+-----------+
|dno|      dname|
+---+-----------+
| 10|Engineering|
| 30|      Sales|
| 40|         HR|
| 50|    Finance|
+---+-----------+
#1. emp1:
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
#2. dept1:
+------+-----------+
|deptno|      dname|
+------+-----------+
|    10|Engineering|
|    30|      Sales|
|    40|         HR|
|    50|    Finance|
+------+-----------+

### Equi Join ###
+------+---+---+------+-----------+
|  name|age|dno|deptno|      dname|
+------+---+---+------+-----------+
| ashok| 33| 40|    40|         HR|
|  naga| 30| 10|    10|Engineering|
|  hari| 24| 10|    10|Engineering|
|  siva| 26| 30|    30|      Sales|
|rajesh| 28| 30|    30|      Sales|
+------+---+---+------+-----------+

### Equi Join ###
+---+------+---+-----------+
|dno|  name|age|      dname|
+---+------+---+-----------+
| 40| ashok| 33|         HR|
| 10|  naga| 30|Engineering|
| 10|  hari| 24|Engineering|
| 30|  siva| 26|      Sales|
| 30|rajesh| 28|      Sales|
+---+------+---+-----------+

### Left Outer Join ###
+---+------+---+-----------+
|dno|  name|age|      dname|
+---+------+---+-----------+
| 20|  ravi| 32|       null|
| 40| ashok| 33|         HR|
| 10|  naga| 30|Engineering|
| 10|  hari| 24|Engineering|
| 30|  siva| 26|      Sales|
| 30|rajesh| 28|      Sales|
+---+------+---+-----------+

### Right Outer Join ###
+---+------+----+-----------+
|dno|  name| age|      dname|
+---+------+----+-----------+
| 40| ashok|  33|         HR|
| 10|  naga|  30|Engineering|
| 10|  hari|  24|Engineering|
| 50|  null|null|    Finance|
| 30|  siva|  26|      Sales|
| 30|rajesh|  28|      Sales|
+---+------+----+-----------+

### Full Outer Join ###
+---+------+----+-----------+
|dno|  name| age|      dname|
+---+------+----+-----------+
| 20|  ravi|  32|       null|
| 40| ashok|  33|         HR|
| 10|  naga|  30|Engineering|
| 10|  hari|  24|Engineering|
| 50|  null|null|    Finance|
| 30|  siva|  26|      Sales|
| 30|rajesh|  28|      Sales|
+---+------+----+-----------+

### Self Join ###
+---+------+---+------+---+
|dno|  name|age|  name|age|
+---+------+---+------+---+
| 20|  ravi| 32|  ravi| 32|
| 40| ashok| 33| ashok| 33|
| 10|  naga| 30|  naga| 30|
| 10|  naga| 30|  hari| 24|
| 10|  hari| 24|  naga| 30|
| 10|  hari| 24|  hari| 24|
| 30|  siva| 26|  siva| 26|
| 30|  siva| 26|rajesh| 28|
| 30|rajesh| 28|  siva| 26|
| 30|rajesh| 28|rajesh| 28|
+---+------+---+------+---+
```

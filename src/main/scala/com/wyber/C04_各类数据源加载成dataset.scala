package com.wyber

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object C04_各类数据源加载成dataset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

//    loadCsv(spark)
//    loadJson(spark)
//    loadJson2(spark)
//    loadJDBC(spark)
//    writeFileTypes(spark)
//    loadParquet(spark)
    loadOrc(spark)
    spark.close()
  }

  //test
  //读csv文件数据源
  def loadCsv(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read
      .option("header", true)
      .csv("data/sql/wc.txt")

    val df1 = df.toDF("stu_id", "stu_name", "stu_class", "stu_score")
    df1.printSchema()
    df1.show()
  }

  //读简单json数据源
  def loadJson(spark: SparkSession): Unit = {

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType, false),
      StructField("name", DataTypes.StringType, false),
      StructField("age", DataTypes.DoubleType, false),
      StructField("gender", DataTypes.StringType, false)
    ))

    val df = spark.read
      //      .schema(schema)
      .option("inferSchema", false)
      .json("data/sql/stu.txt") //如果不手动传入schema，自动推断schema默认是true(写死，不能改为false)，会额外触发job

    df.printSchema()
    df.show()
  }

  //读复杂嵌套json数据源
  def loadJson2(spark: SparkSession): Unit = {

    //{"id":1,"name":"aa","scores":[95,80,86,87],"age":18,"gender":"male"}
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType, false),
      StructField("school",
        DataTypes.createStructType(
          Array(
            StructField("name", DataTypes.StringType),
            StructField("graduate", DataTypes.StringType),
            StructField("rank", DataTypes.IntegerType)
          )
        )
      ),
      StructField("name", DataTypes.StringType, false),
      StructField("info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("score", DataTypes.createArrayType(DataTypes.DoubleType), false), //数组类型
      StructField("age", DataTypes.DoubleType, false),
      StructField("gender", DataTypes.StringType, false)
    ))

    val df = spark.read
      .schema(schema)
      //      .option("inferSchema",false)
      .json("data/sql/stu2.txt") //如果不手动传入schema，自动推断schema默认是true(写死，不能改为false)，会额外触发job

    //    df.printSchema()
    //    df.show(false)

    //TODO 计算每所学校毕业生的平均年龄
    df.createTempView("t0")
    val res = spark.sql(
      """
        |select
        |   school.name as school_name,
        |   avg(age) as avg_age
        |from t0
        |group by school.name
        |""".stripMargin
    )

    res.show(100, false)

  }

  //读jdbc读表数据源
  def loadJDBC(spark: SparkSession): Unit = {
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    val df: DataFrame = spark.read.jdbc(
      "jdbc:mysql://localhost:3306/test",
      "user_profile",
      properties
    )
    //TODO    计算answer_cnt最大的前三人  ---》 全局TopN
    //        计算每种性别年龄中answer_cnt最大的前2人 ---》分组 TopN
    df.createTempView("t0")
    val res1 = spark.sql(
      """
        |select
        |   id,
        |   device_id,
        |   university,
        |   answer_cnt
        |from t0
        |order by answer_cnt desc
        |limit 3
        |""".stripMargin)

    val res2 = spark.sql(
      """
        |select
        |   id,device_id,university,gender
        |from (
        |   select
        |       id,
        |       device_id,
        |       university,
        |       gender,
        |       row_number() over(partition by gender order by answer_cnt desc) as rank
        |   from t0
        |) t1
        |where rank <=2
        |""".stripMargin)
    val res3 = spark.sql(
      """
        |select
        |   id,
        |   device_id,
        |   university,
        |   gender,
        |   row_number() over(partition by gender order by answer_cnt desc) as rank
        |from t0
        |""".stripMargin
    )
    res1.show()
    res2.show()
    res3.show()
    //    df.printSchema()
    //    df.show()
  }

  //写各类文件
  def writeFileTypes(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("id", DataTypes.StringType),
      StructField("name", DataTypes.StringType),
      StructField("age", DataTypes.DoubleType),
      StructField("gender", DataTypes.StringType),
      StructField("school", DataTypes.createStructType(
        Array(
          StructField("name", DataTypes.StringType),
          StructField("graduate", DataTypes.IntegerType),
          StructField("rank", DataTypes.IntegerType)
        )
      )),
      StructField("info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("scores", DataTypes.createArrayType(DataTypes.DoubleType))
    ))

    val df: DataFrame = spark.read.schema(schema).json("data/sql/stu2.txt")

    //将df写成各类结构
    df.write.parquet("data/sql/out-parquet/")
    df.write.json("data/sql/out-json/")
    df.write.orc("data/sql/out-orc/")
    //    df.write.csv("data/sql/out-csv/") 简单结构的dataframe才能写成csv文件，不支持struct，map，array等复合类型
    val df2 = df.drop("info", "scores", "school")
    df2.write.csv("data/sql/out-csv/")


  }

  // 读Parquet文件
  // parquet不需要手动传入schema，因为parquet是自我描述类型的列式存储文件格式（文件内部就带有表结构信息）
  def loadParquet(spark: SparkSession): Unit = {
    val df = spark.read.parquet("data/sql/out-parquet")
    df.printSchema()
    df.show()
  }

  // 读Orc文件
  // Orc不需要手动传入schema，因为Orc是自我描述类型的列式存储文件格式（文件内部就带有表结构信息）
  def loadOrc(spark: SparkSession): Unit = {
    val df = spark.read.orc("data/sql/out-orc")
    df.printSchema()
    df.show()
  }
}

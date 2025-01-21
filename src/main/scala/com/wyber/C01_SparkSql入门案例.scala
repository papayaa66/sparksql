package com.wyber

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.serializer

object C01_SparkSql入门案例 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    /**
     *
     *
     * 编程入口：SparkCore =》SparkContext
     * sparkSql =》sparkSession
     */

    val spark = SparkSession.builder()
      .appName("sql入门")
      .master("local")
      .config("spark.default.parallelism", 4)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //创建Dataset
    val ds: Dataset[String] = spark.read.textFile("data/wordcount/wc.txt")

    //Dataset中就是封装了数据的rdd
    val rdd: RDD[String] = ds.rdd

    //Dataset中还封装了数据表结构的元信息
    ds.printSchema()
    ds.show(2)

    //---------------

    //type DataFrame = Dataset[Row]
    val df: DataFrame = spark.read.csv("data/wordcount/wc.txt")
    //    val df2: Dataset[Row] = spark.read.csv("data/wordcount/wc.txt")

    //DataFrame  也是封装了数据的RDD[Row]
    val rdd1: RDD[Row] = df.rdd
    //DataFrame 还封装了数据映射成表的元信息
    df.printSchema()
    df.show(2)

//      +---+---+------+---+
//      |_c0|_c1|   _c2|_c3|
//      +---+---+------+---+
//      |  1| zs|doit29| 80|
//      |  2| ls|doit29| 90|
//      +---+---+------+---+

    println("-------------------new sql ----")
    //将df注册为一个表
    df.createTempView("t_stu")
    val res: DataFrame = spark.sql(
      """
        |select
        |   _c2 as term,
        |   avg(_c3) as avg_score
        |from t_stu
        |group by _c2
        |""".stripMargin
    )
    res.printSchema()
    res.show()

//    Thread.sleep(Long.MaxValue)
//    spark.close()
  }


}

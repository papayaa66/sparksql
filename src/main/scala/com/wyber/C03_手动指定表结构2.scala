package com.wyber

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C03_手动指定表结构2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("手动指定表结构2——有表头的csv")
      .master("local")
      .getOrCreate()

    val schema: StructType = StructType(Seq(
      StructField("id", DataTypes.IntegerType, false),
      StructField("name",DataTypes.StringType,false),
      StructField("class",DataTypes.StringType,false),
      StructField("power",DataTypes.IntegerType,false)
    ))

    val df = spark.read
      .option("header", true) // 设置，文件有表头
      .schema(schema) // 手动传入的schema（最终的表结构一定是以手动传入的为准！）
      .option("inferSchema", true) // 自动推断schema（生产代码中不要用，因为他会额外出发一个job）
      .csv("data/sql/wc.txt")

    df.printSchema()
    df.show()

    spark.close()
  }


}

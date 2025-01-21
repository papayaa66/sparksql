package com.wyber

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.serializer
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C02_手动指定表结构 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("手动指定表结构")
      .master("local[*]")
      .getOrCreate()

    //创建一个schema（表结构：字段名 字段类型）
    // 1,zs,doit29,80
    val schema: StructType = StructType(Seq(
      StructField("id", DataTypes.IntegerType, false),
      StructField("name", DataTypes.StringType, false),
      StructField("class", DataTypes.StringType, false),
      StructField("score", DataTypes.DoubleType, false)
    ))

    //加载csv结构数据的数据文件成为一个dataset
    val ds = spark.read.schema(schema).csv("data/wordcount/wc.txt")

    ds.printSchema()
    ds.show()

    spark.close()
  }

}

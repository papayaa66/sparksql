package main.scala.com.wyber

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

object C08_Dataset转RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[DoitTeacher] = spark.createDataset(Seq(DoitTeacher(1, "ww", 1.1), DoitTeacher(2, "ss", 1.2), DoitTeacher(3, "xx", 2.2)))
    //ds 转 rdd
    val rdd: RDD[DoitTeacher] = ds.rdd

//    rdd.filter(teacher => teacher.power > 1)

    //ds 转 df
    val df: Dataset[Row] = ds.toDF() //DataFrame == Dataset[Row]

    //df 转 rdd
    val rdd1: RDD[Row] = df.rdd

    //df 转到ds，需要把通用的类型Row，转成自定义类型DoitTeacher
    //需要定义类型对应的Encoder
    val ds2: Dataset[DoitTeacher] = df.as[DoitTeacher](Encoders.product)
    val ds3: Dataset[(Int, String, Double)] = df.as[(Int, String, Double)](Encoders.product)
  }

}

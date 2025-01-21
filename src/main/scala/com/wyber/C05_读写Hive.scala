package main.scala.com.wyber

import org.apache.spark.sql.SparkSession

object C05_读写Hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .config("hive.metastore.uris","thrift://121.36.201.209:9083") //链接集群的方法1，第二种是在resources写一个hive-site.xml
      .appName("")
      .master("local")
      .enableHiveSupport() //读写hive，则需开启hive支持
      .getOrCreate()


    val df = spark.read.table("x02_stu")
    df.printSchema()
    df.show()

    val res = spark.sql(
      """
        |show tables
        |""".stripMargin)
    res.show()

    //    val df = spark.read.json("data/sql/stu.txt")
    //    df.write.saveAsTable("x02_stu") //保存为hive表
  }


}

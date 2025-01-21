package main.scala.com.wyber

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object X01_随堂练习_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .config("spark.sql.shuffle.partitions",1)
      .getOrCreate()

    //TODO 1.把mysql中的一个表映射成sparkSql的 dataframe
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/test", "t_md_areas", props)

    //TODO 2.利用sparkSql 进行查询（查询到所有3级，4级行政单位地点）例如下述
    //  江苏省，盐城市，大丰区，120.xxxx，33，xxxxx
    df.createTempView("t_md_areas")
    val res: DataFrame = spark.sql(
      """
        |select
        |    level_1.areaname AS  province,
        |    level_2.areaname as city,
        |    level_3.areaname as regin,
        |    level_3.bd09_lng as lng,
        |    level_3.bd09_lat as lat                                                                                                                           
        |from t_md_areas level_3
        |   join t_md_areas level_2 on level_3.parentid=level_2.ID AND level_3.LEVEL=3
        |   join t_md_areas level_1 on level_2.parentid=level_1.ID
        |
        |UNION ALL
        |SELECT
        |    level_1.areaname AS  province,
        |    level_2.areaname as city,
        |    level_3.areaname as regin,
        |    level_4.bd09_lng as lng,
        |    level_4.bd09_lat as lat
        |from t_md_areas level_4
        |	JOIN t_md_areas level_3 on level_4.PARENTID=level_3.ID AND level_4.LEVEL=4
        |	JOIN t_md_areas level_2 on level_3.PARENTID=level_2.ID
        |	JOIN t_md_areas level_1 on level_2.PARENTID=level_1.ID
        |""".stripMargin)



    //TODO 3.结果保存为parquet文件，也写入mysql
    res.write.parquet("data/x01_out/")
    res.write.jdbc("jdbc:mysql://localhost:3306/test", "x01_out", props)
  }

}

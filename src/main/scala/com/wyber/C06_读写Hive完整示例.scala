package main.scala.com.wyber

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object C06_读写Hive完整示例 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("")
      .master("local")
      .getOrCreate()

    //读hive表
    val df = spark.read.table("shop_sale")
    df.createTempView("shop_sale")

    val res: DataFrame = spark.sql(
      """
        |-- insert into table shop_accu_sale -- 可以通过insert语句直接将计算结果存到hive的表（要求表已存在）
        |select
        |    shop_id,
        |    month,
        |    amount_month,
        |    sum(amount_month) over(partition by shop_id order by month rows between unbounded preceding and current row) as amount_accu
        |from (
        |    select
        |        shop_id,
        |        month,
        |        sum(amount) as amount_month
        |    from shop_sale
        |    group by shop_id, month
        |) o
        |""".stripMargin)

    // 将结果写入hive
    res.write.mode(SaveMode.Append).saveAsTable("shop_accu_sale")

    spark.close()


  }

}

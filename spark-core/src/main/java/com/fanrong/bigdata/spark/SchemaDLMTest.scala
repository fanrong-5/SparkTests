package com.fanrong.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
 * @author fanrong
 * @create 2021/8/31 12:07 下午
 */
object SchemaDLMTest
{
  def main(args: Array[String]): Unit =
  {

    val spark = SparkSession.builder()
      .appName("SchemaDLMTest")
      .config("spark.sql.execution.arrow.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val date = args(0)

    val testSQL =
      """
        |SELECT
        |get_json_object(param['predict_value'],'$.concatenate') as predict_value,
        |param['passenger_id'] as passenger_id
        |FROM kflower_strategy.ods_log_kflower_ether_uplift_new_pas_luban_ether
        |WHERE concat_ws('-', year, month, day) = '""".stripMargin + date + """'
        |""".stripMargin

    val testDF = spark.sql(testSQL)
    testDF.show(false)
    testDF.printSchema()

    spark.stop()

  }


}

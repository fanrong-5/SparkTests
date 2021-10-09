package com.fanrong.bigdata.spark

import org.apache.spark
import org.apache.spark.sql.types.{ByteType, FloatType, ShortType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import SparkContext._

object Test0
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("FeatureDistributionDetection_test0").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val testDF = spark.read.json("file:///Users/didi/Desktop/data.json")

    testDF.show()
    testDF.printSchema()
    /**
     * 注册临时表
     */
    testDF.registerTempTable("testtable")
    val result  = spark.sql("select  * from jtable")
    result.show()

    // TODO 关闭连接
    spark.stop()


  }


}

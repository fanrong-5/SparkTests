package com.fanrong.bigdata.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MaxAndMin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MaxAndMin")
    val sc = new SparkContext(conf)


  }

}

package com.fanrong.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object TopK {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopK")
    val sc = new SparkContext(conf)
    var num = 0

    sc.setLogLevel("error")

    val lines = sc.textFile("file:///Users/didi/Projects/data/top")
    /*
    * 过滤无效数据
    * 将每行数据进行分割并取出第三个元素
    * 转化为键值对的形式
    * 排序
    * 取出key值*/
    val results = lines.filter(line=>line.trim().length>0)
      .map(_.split(",")(2))
      .map(x=>(x.toInt," "))
      .sortByKey(false)
      .map(x=>x._1)
      .take(5)
      .foreach(x=>{
        num = num +1
        println(num+"\t"+x)

      })


  }


}

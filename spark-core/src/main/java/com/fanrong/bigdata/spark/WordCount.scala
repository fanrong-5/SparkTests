package com.fanrong.bigdata.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 应用
    // spark框架
    // TODO 建立和spark框架的连接
    //JDBC:connection

    val sparConf=new SparkConf()
      .setAppName("WordCount")

    val sc = new SparkContext(sparConf)

    val lines=sc.textFile(path = "datas")
    val wordCount=lines.flatMap(line=>line.split(" "))
      .map(word=>(word,1))
      .reduceByKey((a,b)=>a+b)

    // 打印结果
    val array = wordCount.collect()
    array.foreach(println)



    // TODO 关闭连接
    sc.stop()

  }

}

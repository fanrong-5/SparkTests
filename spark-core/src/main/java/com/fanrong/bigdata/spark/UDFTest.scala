package com.fanrong.bigdata.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}



object UDFTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDFTest").setMaster("local")
    val sc = new SparkContext(conf)

    // 注意：sparkSession的创建方式
    val spark:SparkSession= SparkSession.builder().config(conf).getOrCreate()
    sc.setLogLevel("error")

/*

    // TODO：1-创建测试用DataFrame

    // 构造测试数据，有两个字段：名字和年龄
    val userData = Array(("Leo", 16),("Marry", 21), ("Jack", 14), ("Tom", 18))   // 创建RDD
    val userDF = spark.createDataFrame(userData).toDF("name","age")   // 将RDD转化为DataFrame
    userDF.show

    // 注册一张user表
    userDF.createOrReplaceTempView("user")

    // TODO:spark SQL 用法：注册UDF、使用UDF

    // 1.1-通过匿名函数注册UDF：计算某列的长度，该列的类型是string
    spark.udf.register("strlen",(str:String) => str.length())
    spark.sql("select name,strlen(name) as name_len from user").show

    // 1.2-根据年龄大小返回是否成年 成年：true,未成年：false
    def isAdult(age: Int) = {
      if (age < 18) {
        false
      } else {
        true
      }
    }

    // TODO:DataFrame 用法：注册UDF、使用UDF

*/

    // TODO 2-mkString的用法
    val str = Array("one","two","three")
    val qt = Array(Array("a","b"),Array("one","two"))

    val str1 = str.mkString
    val str2 = str.mkString(",")
    val str3 = str.mkString(" ")
    val str4 = str.mkString("[",",","]")
    val str5 = qt.flatten.mkString(",")

    println("str1="+str1)
    println("str2="+str2)
    println("str3="+str3)
    println("str4="+str4)
    println("str5="+str5)

    // TODO toString的用法
    val v = Vector("apple","banana","cherry")
    val v1 =  v.toString
    print(v1)


    spark.stop()
  }


}

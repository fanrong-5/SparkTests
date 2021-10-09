package com.fanrong.bigdata.spark

import breeze.linalg.Axis._0
import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonArray, JsonParser, JsonPrimitive}
import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, col, expr, first, format_number, lit, monotonically_increasing_id, nanvl, sum}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import shapeless.ops.nat.GT.>
import shapeless.ops.nat.LT.<
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.DoubleType
import org.json.JSONArray

import scala.collection.JavaConversions.`deprecated asScalaBuffer`
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author fanrong
 * @create 2021/8/26 3:28 下午
 */
object DLMTest
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("DLM").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("FeatureDistributionDetection_test1").getOrCreate()
    sc.setLogLevel("error")

    val testDF = spark.read.json("test.json")
    testDF.show(false)
    testDF.printSchema()

    spark.udf.register("prediction_udf", predictionUDF(_: String, _: Int))
    spark.udf.register("get_finish_rate", getFinishRateUDF(_: String, _: Int))



    // todo 将coupon列转化为整型变量


    /*   val coupons = testDF.select("coupon").collect()
       val predictionValue = testDF.select("prediction").collect()
       var index = 0

           for(coupon <- coupons){

             val couponIndex = coupon(index)  // 获取coupon值
             val predictions = predictionValue(index)  // 获取prediction_value 值
             val prediction = expr(s"predictionTestUDF($predictions,$couponIndex)")

            /* testDF.withColumn("prediction_value",expr(s"prediction_udf($testDF,$couponIndex)"))
             index = index +1

           }*/
   */
    // TODO 思路一：循环遍历testDF 将 df 转化为 array ,遍历 array

    import spark.implicits._


    // 创建一个空列 类型为double

    /*
        println("---------------------------------------------为dataframe添加新列--------------------------------------------")
        val null_udf = udf((s: Any) => null)

        testDF.withColumn("prediction_value", null_udf(col("coupon")).cast(DoubleType)).show(false)*/
    println("---------------------------------------------udf--------------------------------------------")

    val newDF = testDF.withColumn("prediction_value", expr("get_finish_rate(prediction,coupon)"))
    val resultDF = testDF.map(row =>
    {
      val index = row.getAs[Long]("coupon")
      val listStr = row.getAs[String]("prediction")
      val gson = new Gson()
      import collection.JavaConverters._
      val list: java.util.List[Double] = gson.fromJson(listStr, new TypeToken[java.util.List[Double]]()
      {}.getType())
      (index, listStr, list.get(index.toInt))
    }).toDF("coupon","prediction","prediction_value")

    println("---------------------------------------------test--------------------------------------------")
    resultDF.show(false)

    println("---------------------------------------------测试Scala的条件语句--------------------------------------------")

    val features = Array(0,1,2,3,4,5,6)


    for(feature <- features){
      if(feature > 3 || feature <1){
        println(feature)
      }
    }





    /*
        println("---------------------------------------------转换数据帧到RDD（列表），然后回数据框,可以获取相应的list--------------------------------------------")

        val predictionDF = testDF.select("prediction").rdd.map(_.getList[Double](0).toList).toDF("prediction").collect()
        val couponDF = testDF.select("coupon").collect()

        for (i <- 0 to predictionDF.length - 1)
        {

          val coupon = couponDF(i)(0).toString.toInt
          val testList = predictionDF(i).getList(0)

          val prediction = expr(s"predictionUDF($testList,$coupon)")

          println(prediction)

        }
    */


    /*   map(x=>{


       val predictions = x(0)
       val coupon = x(1)
       val prediction = predictions(coupon)

       (predictions,coupon,prediction)

     }).toDF("predictions","coupon","prediction")

     test.show(10)*/


    spark.stop()

  }


  def predictionUDF(predictionValue: String, coupon: Int) =
  {

    // 获取相应的预测值
    val result = predictionValue(coupon).toDouble
    result

    // todo 处理coupon空值情况
  }


  def getFinishRateUDF(predictionValue: String, coupon: Int) =
  {

    val jsonParser: JsonParser = new JsonParser()

    val double = jsonParser.parse(predictionValue).asInstanceOf[JsonArray]
    // todo 由coupon得到索引 转为int

    val index = coupon
    // todo 处理coupon空值情况

    val prediction = double.get(index).getAsDouble
    println(prediction)

    prediction

  }


}

package com.fanrong.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
 * @author fanrong
 * @create 2021/9/23 3:34 下午
 */
object NewDataframeTest{
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("NewDataframeTest").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("NewDataframeTest").getOrCreate()
    sc.setLogLevel("error")


    /**
     * 创建新的空的dataframe
     * 可以给每列指定相对应的类型
     */

    import spark.implicits._

    val schema1 = StructType(
      Seq(
        StructField("feature", StringType, true),
        StructField("null_value_fluctuation", StringType, true),
        StructField("maxFluctuation", DoubleType, true),
        StructField("nonNull_value_alarm", StringType, true)))
    var emptyDf1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)

    emptyDf1.show

    val features  = Array("sex","name","weight","tall","teach","num","stu")
    var num = 53
    val n = features.length -1

    for( feature <- features){
      val tempDF = Seq((feature,"danny",num,"tall"))
      .toDF("feature","null_value_fluctuation","maxFluctuation","nonNull_value_alarm")
      num = num - 1

      emptyDf1 = emptyDf1.union(tempDF)
    }

    emptyDf1.show()

/*    emptyDf1.map(row =>{
      val abnormalFeature = row.getAs[String]("feature")
      val nullValueFluc = row.getAs[String]("null_value_fluctuation")
      val alarm = row.getAs[String]("nonNull_value_alarm")
      println("-----------异常值报警-----------")
      println(abnormalFeature)
      println(nullValueFluc)
      println(alarm)
      (abnormalFeature, nullValueFluc, alarm)
    })*/

    val test = emptyDf1
      .orderBy("maxFluctuation")
      .selectExpr(
        "collect_list(feature) over(  order by maxFluctuation) ",
        "collect_list(null_value_fluctuation) over(  order by maxFluctuation) ",
        "collect_list(maxFluctuation) over( order by maxFluctuation)",
        "collect_list(nonNull_value_alarm) over(  order by maxFluctuation)" )
      .collect()


    val len = test(0).length - 1
    println(len)

    val abnormalFeatures = test(n).getSeq[String](0)
    val nullValueFluctuations = test(n).getSeq[String](1)
    val max = test(n).getSeq[Integer](2)
    val alarms = test(n).getSeq[String](3)

    println(abnormalFeatures.indices)

    for (i <- abnormalFeatures.indices) {

      println("-----------异常值报警-----------")
      println(abnormalFeatures(i))
      println(nullValueFluctuations(i))
      println(max(i))
      println(alarms(i))

    }

    spark.stop()

  }
}

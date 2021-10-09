package com.fanrong.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{abs, col, expr, first, format_number, lit, monotonically_increasing_id, nanvl, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Test1
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("FeatureDistributionDetection_test1").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("FeatureDistributionDetection_test1").getOrCreate()

    sc.setLogLevel("error")

    //  TODO 1-将json文件转化为dataframe
    val offlineDF = spark.read.json("data1.json")
    val onlineDF = spark.read.json("data2.json")

    // TODO 测试
    println("-----------------------测试 筛选条件----------------------------")
    onlineDF.show()

    val diffThreshold = 30

    val results = onlineDF.filter(expr(s" age > $diffThreshold"))
      .select(s"age", s"order_num")
      .selectExpr(s"collect_list(order_num)", s"collect_list(age)")
      .collect()

    println(results.length)

    if(results.length > 0) {

      println("限制条件无效")
      val metrics = results(0).getSeq[Long](0)
      val percents = results(0).getSeq[Long](1)
      var resultStr = collection.mutable.ArrayBuffer[String]()
      for (i <- metrics.indices) {
        val metric = metrics(i)
        val percent = percents(i)
        resultStr += s"${metric}相差${percent}%"
      }

      // 解决方法一
      if(resultStr.length == 0){
        println("无有效结果")
      }
      if(resultStr.length > 0){
        print(resultStr.length)
        println(" 有有效结果")
      }
      println(resultStr.mkString(","))




    }else{
      println("限制条件有效")
    }

    var resultStr = collection.mutable.ArrayBuffer[String]()

    for(i <- 10 to 15){
      resultStr += s"他今年${i}岁了"
    }

    println(resultStr.mkString(","))
    println(resultStr.length)
    println(resultStr.size)


    // offlineDF.show(3,false)  // 对过长字符串的显示格式

    //  TODO 2-判断某列是否存在
    implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame)
    {
      def hasColumn(colName: String) = df.columns.contains(colName)
    }

    if (offlineDF.hasColumn("agea"))
    {
      println("The column exists")
    } else
    {
      println("The column does not exist")
    }

    // 计算空值占比
   /*  val result = offlineDF.agg(*[
       (1-(fn.count(c)/fn.count('*'))).alias(c + '_missing')
        for c in offlineDF.columns])
*/

   /* println(offlineDF.isnull(col("sex")).sum())*/

    /*val countNullSql = offlineDF.withColumn("nullCount",expr("select 1 "))*/

    val resultOFF = offlineDF
      .agg((sum(when(col("age").isNotNull,0).otherwise(1))/count("*")).as("off_null"))

    val resultON = onlineDF
      .agg((sum(when(col("age").isNotNull,0).otherwise(1))/count("*")).as("on_null"))
      .selectExpr("*", "concat(round(on_null*100,2),'%') as on_null")

    // TODO expr方法


    resultON.show()


  /*  sum(col("age").isNull.asInstanceOf[IntegerType])
      .divide(count(lit(1)))
      .as("age:percent null")*/



    //  TODO  3-标签类型： 计算占比

    // TODO 3.1-思路一：先join 再计算占比

    val joinDF = offlineDF.join(onlineDF, offlineDF("sex") === onlineDF("sex"), "fullouter")
    // 选取列 重命名 计算占比
    joinDF.show()

    // TODO 3.2-思路二：先计算占比 再join

    val offlineDF1 = offlineDF.groupBy("sex")
      .count()
      .withColumn("off_percentage", format_number(col("count")
        .divide(sum("count").over)
        .multiply(100), 5))

    val onlineDF1 = onlineDF.groupBy("sex")
      .count()
      .withColumn("on_percentage", format_number(col("count")
        .divide(sum("count").over)
        .multiply(100), 5))
      .select("age")

      .limit(2)

    offlineDF1.show()
    onlineDF1.show()

    // 将在线 离线的两个dataframe 合并

    // 方法一（待改进）
    /*val result0 = offlineDF1.withColumn("id",monotonically_increasing_id())
      .join(onlineDF1.withColumn("id",monotonically_increasing_id()),Seq("id"))
      .drop("id")
*/
    // 方法二（外连接）
    val result0 = offlineDF1.join(onlineDF1, offlineDF1("sex") === onlineDF1("sex"), "fullouter")


    // 计算差值

    //    val result1 = result0.withColumn("diff",abs(nanvl(result0.col("off_percentage"),0)-nanvl(result0.col("on_percentage"),lit("0"))))
    val result1 = result0
      .withColumn("diff", expr("abs(nvl(off_percentage,0) - nvl(on_percentage,0))"))
      .filter(col("diff") > 20)
      .sort(col("diff").desc)
      .selectExpr("*", "concat(round(diff*1,2),'%') as diff1")
      .drop("diff")

    println("-------------------标签类型字段差值-------------------")

    result1.show()


    /*// 按照年龄分组
    testDF.groupBy(testDF.col("age"))
      .count()
      .sort(testDF("age").desc)
      .show();*/

    //  TODO  3-数值类型： 计算均值、25%分位数、中位数、75%分位数、方差

    val offNumDF = spark.read.json("file:///Users/didi/Desktop/fanrong/projects/testdata/data_test1.json")
    val onNumDF = spark.read.json("file:///Users/didi/Desktop/fanrong/projects/testdata/data_test2.json")

    /*    // TODO 3.1-离线数据

        // 测试describe函数：均值 方差
        val describeDF = offNumDF.describe()
        // 转换结果

        import spark.implicits._

        val schema = describeDF.schema

        val longForm = describeDF
          .flatMap(row => {
            val metric = row.getString(0)
            (1 until row.length).map(i => {
            (metric, schema(i).name, row.getString(i).toDouble)
          })
        }).toDF("summary", "attribute", "values")

        // 列联表
        val meanDF = longForm
          .groupBy("attribute")
          .pivot("summary")
          .agg(first("values"))
          .drop("count","max","min")

        meanDF.show()


       /* 测试approxQuantile方法 ：分位数
       * 中位数：第二四分位数（Q2），也叫二分位数
       * 25%分位数：第一四分位数（Q1）
       * 75%分位数：第三四分位数（Q3）
       * */

        val numType = Array(DoubleType, IntegerType, LongType, DecimalType, FloatType)
        val tuples = offNumDF.schema
          .map(sf => {
          (sf.name, sf.dataType)
        })
          .filter(tp => {
          numType.contains(tp._2)
        })
          .map(scm => {
          val quantiles = offNumDF .stat.approxQuantile(scm._1, Array(0.25, 0.5, 0.75), 0)
          val col = scm._1
          (col, quantiles)
        })



        val quantileDF = tuples
          .toDF("attribute", "quantiles")
          .withColumn("Q1", col ("quantiles").getItem(0))   // 25%分位数
          .withColumn("Q2",col("quantiles").getItem(1))     // 中位数
          .withColumn("Q3",col( "quantiles").getItem(2))    // 75%中位数
          .drop("quantiles")

        quantileDF.show()

        // 连接 【均值 方差】表 和 【分位数】表
        quantileDF.join(meanDF,"attribute").show*/
    // val quantiles =  offNumDF.stat.approxQuantile(Array("age","order_num"),Array(0.25,0.5,0.75),0.25)

    // TODO 2021/07/1 测试新思路

    /*
        // 创建新的dataFrame
        import spark.implicits._

        val fieldDF = Seq(("mean"),("std"),("Q1"),("Q2"),("Q3")).toDF("count").show()
    */

    // 计算离线表的字段数值
    val offlineCount = offNumDF
      .selectExpr("avg(age) as off_a ", "avg(order_num) as off_b").withColumn("off_statistics", lit("均值"))
      .unionAll(offNumDF.selectExpr("stddev(age) as off_a ", "stddev(order_num) as off_b").withColumn("off_statistics", lit("方差")))
      .unionAll(offNumDF.selectExpr("percentile(age,0.25) as off_a", "percentile(order_num,0.25) as off_b").withColumn("off_statistics", lit("25%分位数")))
      .unionAll(offNumDF.selectExpr("percentile(age,0.5) as off_a", "percentile(order_num,0.5) as off_b").withColumn("off_statistics", lit("中位数")))
      .unionAll(offNumDF.selectExpr("percentile(age,0.75) as off_a", "percentile(order_num,0.75) as off_b").withColumn("off_statistics", lit("75%分位数")))


    val onlineCount = onNumDF
      .selectExpr("avg(age) as on_a ", "avg(order_num) as on_b").withColumn("on_statistics", lit("均值"))
      .unionAll(onNumDF.selectExpr("stddev(age) as on_a ", "stddev(order_num) as on_b").withColumn("on_statistics", lit("方差")))
      .unionAll(onNumDF.selectExpr("percentile(age,0.25) as on_a", "percentile(order_num,0.25) as on_b").withColumn("on_statistics", lit("25%分位数")))
      .unionAll(onNumDF.selectExpr("percentile(age,0.5) as on_a", "percentile(order_num,0.5) as on_b").withColumn("on_statistics", lit("中位数")))
      .unionAll(onNumDF.selectExpr("percentile(age,0.75) as on_a", "percentile(order_num,0.75) as on_b").withColumn("on_statistics", lit("75%分位数")))



        offlineCount.show()
        onlineCount.show()



    /*

        val offlineCount = offNumDF.withColumn("statistics",lit("mean"))
          .select(col("statistics"),expr("avg(age) as a "),expr("avg(order_num) as b"))
          .unionAll(offNumDF
            .withColumn("statistics",lit("stddev"))
            .select(col("statistics"),expr("stddev(age) as a "),expr("stddev(order_num) as b")))
          .unionAll(offNumDF
            .withColumn("statistics",lit("Q1"))
            .select(col("statistics"),expr("percentile(age,0.25) as a"),expr("percentile(order_num,0.25) as b")))
          .unionAll(offNumDF
            .withColumn("statistics",lit("Q2"))
            .select(col("statistics"),expr("percentile(age,0.5) as a"),expr("percentile(order_num,0.5) as b")))
          .unionAll(offNumDF
            .withColumn("statistics",lit("Q3"))
            .select(col("statistics"),expr("percentile(age,0.75) as a"),expr("percentile(order_num,0.75) as b")))
          .show()

    */

    // 注意：在select方法中，不能在其中混合使用【直接选择列】、【函数】两种方法

    // TODO 计算在线和离线的数据之差

    val countDIFF = offlineCount.join(onlineCount, offlineCount("off_statistics") === onlineCount("on_statistics"))
      .withColumn("diff_a", abs((col("off_a") - col("on_a")) / (col("off_a") + col("on_a")) * 2))
      .withColumn("diff_b", abs((col("off_b") - col("on_b")) / (col("off_b") + col("on_b")) * 2))
      .selectExpr("off_statistics as statistics", "off_a", "on_a", "off_b", "on_b", "diff_a", "diff_b")

    // .selectExpr("off_statistics as statistics","off_a","on_a","off_b","on_b","concat(round(diff_a*100,2),'%') as diff_a","concat(round(diff_b*100,2),'%') as diff_b")


    println("-------------------数值类型字段差值-------------------")
    countDIFF.show()

    // TODO 5-输出：倒序输出超出一定值的字段信息

    /*

        val diffA = countDIFF.filter(countDIFF("diff_a")>0.2)
          .sort(col("diff_a").desc)
          .selectExpr("statistics","off_a","on_a","concat(round(diff_a*100,2),'%') as diff_a")


        val diffB = countDIFF.filter(countDIFF("diff_b")>0.2)
          .sort(col("diff_b").desc)
          .selectExpr("statistics","off_b","on_b","concat(round(diff_b*100,2),'%') as diff_b")


        diffA.show()
        diffB.show()

    */

    val diffAA = countDIFF.filter(countDIFF("diff_a") > 0.2)
      .select("diff_a", "statistics")
      .selectExpr("collect_list(statistics)")
      .collect()(0).getSeq[String](0)
      .mkString(",")

    println(diffAA + "超过了20%")


    // TODO 6-机器人报警


    // TODO 中断连接
    spark.stop()
  }

}

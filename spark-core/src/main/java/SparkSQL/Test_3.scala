package SparkSQL

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, expr, sum, when}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import java.util
import scala.collection.mutable.ArrayBuffer

object Test_3
{
  def main(args: Array[String]): Unit =
  {

    val spark = SparkSession.builder()
      .appName("Test_3")
      .config("spark.sql.execution.arrow.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // TODO 获取数据

    val preDate = args(0)
    val date = args(1)
    val diffThreshold = args(2).toDouble
    val notNULLThreshold = args(3).toDouble
    val webHook = args(4)

    val preFeatureSelectSQL =
      """
        |SELECT param['字段名'] as a
        |FROM 表1
        |WHERE  concat_ws('-', year, month, day) = '""".stripMargin + preDate + """'
                                                                                  |""".stripMargin

    val featureSelectSQL =
      """
        |SELECT param['字段名'] as a
        |FROM 表2
        |WHERE  concat_ws('-', year, month, day) = '""".stripMargin + date + """'
                                                                               |""".stripMargin

    var preFeatureDF = spark.sql(preFeatureSelectSQL)
    var featureDF = spark.sql(featureSelectSQL)

    println("-------------------------------------------------------sql结果-------------------------------------------------------")
    preFeatureDF.show(false)
    featureDF.show(false)

    // 一对一

    val features = Array(
      "name",
      "age"
    )

    println("-------------------------------------------------------特征字段个数-------------------------------------------------------")
    println(features.length)

    spark.udf.register("feature_udf", featureUDF(_: String, _: Int))

    val featureCols = ArrayBuffer[String]()
    var index = 0

    for (feature <- features)
    {
      preFeatureDF = preFeatureDF.withColumn(feature, expr(s"cast(feature_udf(predict_value_feature,$index) as double)"))
      featureDF = featureDF.withColumn(feature, expr(s"cast(feature_udf(predict_value_feature,$index) as double)"))
      index += 1
      featureCols += feature
    }

    println("-------------------------------------------------------特征字段拆分后结果-------------------------------------------------------")
    preFeatureDF.show()
    featureDF.show()

    //创建临时表

    val PRE_FEATURE_TABLE = "pre_feature_table"
    val FEATURE_TABLE = "feature_table"

    preFeatureDF.createOrReplaceTempView(PRE_FEATURE_TABLE)
    featureDF.createOrReplaceTempView(FEATURE_TABLE)

    // 报警标题


    var text = s"|  特征字段  |  报警阈值  |  空值占比波动  |  非空值波动  |\n"

    // 计算 非缺失值波动

    var prePeriodNumColAvgExpr = ArrayBuffer[String]()
    var prePeriodNumColPercentile25 = ArrayBuffer[String]()
    var prePeriodNumColPercentile50 = ArrayBuffer[String]()
    var prePeriodNumColPercentile75 = ArrayBuffer[String]()
    var prePeriodNumColStd = ArrayBuffer[String]()

    var thisPeriodNumColAvgExpr = ArrayBuffer[String]()
    var thisPeriodNumColPercentile25 = ArrayBuffer[String]()
    var thisPeriodNumColPercentile50 = ArrayBuffer[String]()
    var thisPeriodeNumColPercentile75 = ArrayBuffer[String]()
    var thisPeriodNumColStd = ArrayBuffer[String]()

    for (feature <- featureCols){

      prePeriodNumColAvgExpr += s"avg(${feature}) as pre_${feature}"
      prePeriodNumColPercentile25 += s"percentile(${feature},0.25) as pre_${feature}"
      prePeriodNumColPercentile50 += s"percentile(${feature},0.5) as pre_${feature}"
      prePeriodNumColPercentile75 += s"percentile(${feature},0.75) as pre_${feature}"
      prePeriodNumColStd += s"round(stddev(${feature}),6) as pre_${feature} "

      thisPeriodNumColAvgExpr += s"avg(${feature}) as this_${feature} "
      thisPeriodNumColPercentile25 += s"percentile(${feature},0.25) as this_${feature}"
      thisPeriodNumColPercentile50 += s"percentile(${feature},0.5) as this_${feature}"
      thisPeriodeNumColPercentile75 += s"percentile(${feature},0.75) as this_${feature} "
      thisPeriodNumColStd += s"round(stddev(${feature}),6) as this_${feature} "

    }

    val STATISTICS = "statistics"

    // 计算上期表字段的 均值、方差、25%分位数、中位数、75%分位数
    println("-------------------------------------------------------上期特征值描述-------------------------------------------------------")

    val prePeriodCount = spark.sql(s"select ${prePeriodNumColAvgExpr.mkString(",")} , '均值' as pre_$STATISTICS from $PRE_FEATURE_TABLE")
      .union(spark.sql(s"select ${prePeriodNumColStd.mkString(",")} , '标准差' as pre_$STATISTICS from $PRE_FEATURE_TABLE"))
      .union(spark.sql(s"select ${prePeriodNumColPercentile25.mkString(",")} , '25分位数' as pre_$STATISTICS from $PRE_FEATURE_TABLE"))
      .union(spark.sql(s"select ${prePeriodNumColPercentile50.mkString(",")} , '中位数' as pre_$STATISTICS from $PRE_FEATURE_TABLE"))
      .union(spark.sql(s"select ${prePeriodNumColPercentile75.mkString(",")} , '75分位数' as pre_$STATISTICS from $PRE_FEATURE_TABLE"))

    prePeriodCount.show()

    // 计算本期表字段的 均值、方差、25%分位数、中位数、75%分位数
    println("-------------------------------------------------------本期特征值描述-------------------------------------------------------")

    val thisPeriodCount = spark.sql(s"select ${thisPeriodNumColAvgExpr.mkString(",")} , '均值' as this_$STATISTICS from $FEATURE_TABLE")
      .union(spark.sql(s"select ${thisPeriodNumColStd.mkString(",")} , '标准差' as this_$STATISTICS from $FEATURE_TABLE"))
      .union(spark.sql(s"select ${thisPeriodNumColPercentile25.mkString(",")} , '25分位数' as this_$STATISTICS from $FEATURE_TABLE"))
      .union(spark.sql(s"select ${thisPeriodNumColPercentile50.mkString(",")} , '中位数' as this_$STATISTICS from $FEATURE_TABLE"))
      .union(spark.sql(s"select ${thisPeriodeNumColPercentile75.mkString(",")} , '75分位数' as this_$STATISTICS from $FEATURE_TABLE"))

    thisPeriodCount.show()

    // join两表 计算两表差值

    var countDIFF = prePeriodCount.join(thisPeriodCount, prePeriodCount(s"pre_$STATISTICS") === thisPeriodCount(s"this_$STATISTICS"))

    for (feature <- featureCols) {

      // 计算差值表达式
      val str = s"abs((this_${feature} - " +
        s"pre_${feature} ) / (this_${feature} + pre_${feature}) * 2 ) * 100"

      // 计算差值
      countDIFF = countDIFF.withColumn(s"diff_${feature}", expr(str))

    }

    val colSeq = collection.mutable.ArrayBuffer[String]()

    colSeq += s"pre_${STATISTICS}"

    for (feature <- featureCols) {
      colSeq += s"diff_${feature}"
    }

    countDIFF = countDIFF.select(colSeq.head, colSeq.tail: _*)

    println("-------------------------------------------------------特征值差值描述-------------------------------------------------------")
    countDIFF.show()

    var abnormalNums = 0             // 记录异常数据的条数
    /* var maxFluctuation = 0.0         // 记录每个特征的最大波动值*/

    // TODO 创建空的dataframe,用于排序

    println("------------------------------------空的dataframe------------------------------------")

    val schema1 = StructType(
      Seq(
        StructField("feature", StringType, true),
        StructField("null_value_fluctuation", StringType, true),
        StructField("maxFluctuation", DoubleType, true),
        StructField("nonNull_value_alarm", StringType, true)))
    var sortDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)

    sortDF.show()

    import spark.implicits._

    for (feature <- featureCols)
    {

      // 计算每一个特征对应的空值占比

      val (preNumNull, numNull) = calCalculateNullResult(preFeatureDF, featureDF, feature)
      val nullDiff = (numNull - preNumNull) * 100 / preNumNull

      // 筛选处符合条件的差值（异常波动的差值）
      // TODO 添加排序条件

      val rows = countDIFF.filter(expr(s"diff_${feature} > $notNULLThreshold"))
        .select(s"diff_${feature}", s"pre_$STATISTICS")
        .orderBy(s"diff_${feature}")
        .selectExpr(s"collect_list(pre_$STATISTICS)", s"collect_list(diff_${feature})")
        .collect()

      // TODO 选择第0组数据，即：均值方差等指标中，波动最大的那个

      val metrics = rows(0).getSeq[String](0)
      val percents = rows(0).getSeq[Double](1)
      var maxFluctuation = 0.0  // 每一个特征的最大波动值
      var resultStr = collection.mutable.ArrayBuffer[String]()
      for (i <- metrics.indices) {
        val metric = metrics(i)   // 指标：均值等
        val percent = percents(i).formatted("%.2f")  // 指标值：均值之差
        resultStr += s"${metric}相差${percent}%"

        if(i == 0) maxFluctuation = percent.toDouble
      }


      if(resultStr.length > 0 || nullDiff.abs >= diffThreshold) {

        abnormalNums += 1

        // TODO 将想要的放入dataframe中

        val tempDF = Seq((feature,nullDiff.formatted("%.2f"),maxFluctuation,resultStr.mkString(",")))
          .toDF("feature","null_value_fluctuation","maxFluctuation","nonNull_value_alarm")

        sortDF =  sortDF.union(tempDF)
      }

    }

    println("-------------------------------------------排序后的dataframe-------------------------------------------")

    sortDF = sortDF.orderBy(sortDF("maxFluctuation").desc)
    sortDF.show(false)

    println("-------------------------------------------异常数据的条数-------------------------------------------")

    println(abnormalNums)

    // TODO 循环遍历排序后的dataframe
    /*
        import spark.implicits._

        sortDF.map(row =>{
          val abnormalFeature = row.getAs[String]("feature")
          val nullValueFluc = row.getAs[String]("null_value_fluctuation")
          val alarm = row.getAs[String]("nonNull_value_alarm")

          text += buildAlarmMessage(abnormalFeature, diffThreshold, nullValueFluc, alarm)

          (abnormalFeature, nullValueFluc, alarm)
        })*/

    val newRows = sortDF
      .selectExpr(
        "collect_list(feature) over (order by maxFluctuation desc)",
        "collect_list(null_value_fluctuation) over (order by maxFluctuation desc) ",
        "collect_list(maxFluctuation) over (order by maxFluctuation desc)",
        "collect_list(nonNull_value_alarm) over (order by maxFluctuation desc)" )
      .collect()

    val len = newRows(0).length - 1
    println(len)

    val n = abnormalNums - 1

    val abnormalFeatures = newRows(n).getSeq[String](0)           // 特征名称
    val nullValueFluctuations = newRows(n).getSeq[String](1)      // 空值占比波动
    val alarms = newRows(n).getSeq[String](3)                     // 非空值占比波动

    for (i <- abnormalFeatures.indices) {
      val abnormalFeature = abnormalFeatures(i)
      val nullValueFluc = nullValueFluctuations(i)
      val alarm = alarms(i)

      text += buildAlarmMessage(abnormalFeature, diffThreshold, nullValueFluc, alarm)

    }

    // 输出任务信息


    // 中断连接

    spark.stop()
  }

  //  获取 特征对应的值

  def featureUDF(featureMap: String, index: Int) =
  {
    val features = featureMap.split(",")
    val str = features(index).split(":")(1)
    str
  }


  /**
   * 计算所有类型的字段空值占比结果
   *
   * @param offlineDF 离线表
   * @param onlineDF  在线表
   * @param x         字段
   * @return 空值占比结果的dataframe
   * */

  private def calCalculateNullResult(preFeatureDF: DataFrame, featureDF: DataFrame, x: String) =
  {

    val preFeatureNull = preFeatureDF
      .agg((sum(when(col(s"${x}") === -1, 1).otherwise(0)) / count("*")).as("pre_feature_null"))
      .selectExpr("round(pre_feature_null * 100, 6) as pre_feature_null")
      .collect()

    val featureNull = featureDF
      .agg((sum(when(col(s"${x}") === -1, 1).otherwise(0)) / count("*")).as("feature_null"))
      .selectExpr("round(feature_null * 100 ,6) as feature_null")
      .collect()

    // TODO 计算差值

    (preFeatureNull(0).getDouble(0), featureNull(0).getDouble(0))
  }

  def hasColumn(df: org.apache.spark.sql.DataFrame, colName: String) = df.columns.contains(colName)

  def buildAlarmMessage(feature: String, diffThreshold: Double, nullDiff: String, notNullDiff: String): String =
  {
    s"|  $feature |  ${diffThreshold}%  |  ${nullDiff}%  |  ${notNullDiff}  |\n"
  }

}

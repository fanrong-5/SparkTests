/*
package SparkSQL
import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, col, count, expr, format_number, lit, monotonically_increasing_id, sum, when}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object FeatureDistributionDetectionV1
{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FeatureDistributionDetection")
      .config("spark.sql.execution.arrow.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val LABEL_TYPE = "LABEL"

    // TODO 输入参数，从args里取
    val dataJson =args(0)
    println(dataJson)

    val inputParam = JsonUtil.jsonToObj(dataJson, classOf[FeatureDistributionDetectionInput])

    // TODO 读取本地数据

    val OFFLINE_TABLE = "offline_table"
    val ONLINE_TABLE = "online_table"

    val offlineDF = spark.sql(inputParam.getOfflineTable)
    offlineDF.createOrReplaceTempView(OFFLINE_TABLE)

    val onlineDF = spark.sql(inputParam.getOnlineTable)
    onlineDF.createOrReplaceTempView(ONLINE_TABLE)

    val fieldList = inputParam.getFields

    // TODO 机器人报警地址，从args取

    val webHook = args(1)


    var text = s"离线表：${inputParam.getOfflineTable}\n在线表：${inputParam.getOnlineTable}\n|  离线表字段  |  在线表字段  |  字段类型  |  报警阈值  |  报警内容 |\n"

    val numCols = ArrayBuffer[Field]()
    val labelCols = ArrayBuffer[Field]()

    for (x <- fieldList) {
      val offlineField = x.getOfflineField
      val onlineField = x.getOnlineField
      val offlineColFlag = hasColumn(offlineDF, offlineField)
      val onlineColFlag = hasColumn(onlineDF, onlineField)

      if (!offlineColFlag) {
        println("离线表字段" + offlineField + "不存在")
        sys.exit(-1)
      }


      if (!onlineColFlag) {
        println("在线表字段" + onlineField + "不存在")
        sys.exit(-1)
      }

      if (LABEL_TYPE.equals(x.getType)) {

        labelCols += x

        // 标签类型的字段的占比
        val diffResult = calCalculateLabelDiffResult(offlineDF, onlineDF, x)

        // 标签类型的字段的空值占比
        val (offLabelNull,onLabelNull) = calCalculateNullResult(offlineDF, onlineDF, x)

        if (!x.getAlarmFilter.isEmpty) {
          val filters = x.getAlarmFilter
          for (filter <- filters) {
            if ("PERCENT".equals(filter.getType)) {
              val value = filter.getValue
              val alarmResult = diffResult.filter(s"diff > $value")
              val cnt = alarmResult.count()
              if (cnt > 0) {
                text += buildAlarmMessage(x.getOfflineField, x.getOnlineField, x.getType, value, s"分布diff超过阈值,离线空值占比为${offLabelNull}%,在线空值占比为${onLabelNull}%")
              }
            }
          }
        }

      } else {
        numCols += x
      }

    }

    var offlineNumColAvgExpr = ArrayBuffer[String]()
    var offlineNumColPercentile25 = ArrayBuffer[String]()
    var offlineNumColPercentile50 = ArrayBuffer[String]()
    var offlineNumColPercentile75 = ArrayBuffer[String]()
    var offlineNumColStd = ArrayBuffer[String]()

    var onlineNumColAvgExpr = ArrayBuffer[String]()
    var onlineNumColPercentile25 = ArrayBuffer[String]()
    var onlineNumColPercentile50 = ArrayBuffer[String]()
    var onlineNumColPercentile75 = ArrayBuffer[String]()
    var onlineNumColStd = ArrayBuffer[String]()

    for (index <- numCols.indices) {
      val field = numCols.get(index)

      //  获取在离线字段

      val offlineField = field.getOfflineField
      val onlineField = field.getOnlineField

      // 存储 计算表达式

      offlineNumColAvgExpr += s"avg(${offlineField}) as off_${offlineField}"
      offlineNumColPercentile25 += s"percentile(${offlineField},0.25) as off_${offlineField}"
      offlineNumColPercentile50 += s"percentile(${offlineField},0.5) as off_${offlineField}"
      offlineNumColPercentile75 += s"percentile(${offlineField},0.75) as off_${offlineField}"
      offlineNumColStd += s"round(stddev(${offlineField}),6) as off_${offlineField} "

      onlineNumColAvgExpr += s"avg(${onlineField}) as on_${onlineField} "
      onlineNumColPercentile25 += s"percentile(${onlineField},0.25) as on_${onlineField}"
      onlineNumColPercentile50 += s"percentile(${onlineField},0.5) as on_${onlineField}"
      onlineNumColPercentile75 += s"percentile(${onlineField},0.75) as on_${onlineField} "
      onlineNumColStd += s"round(stddev(${onlineField}),6) as on_${onlineField} "
    }

    val STATISTICS = "statistics"

    // 计算在线表字段的 均值、方差、25%分位数、中位数、75%分位数

    val offlineCount = spark.sql(s"select ${offlineNumColAvgExpr.mkString(",")} , '均值' as off_$STATISTICS from $OFFLINE_TABLE")
      .union(spark.sql(s"select ${offlineNumColStd.mkString(",")} , '标准差' as off_$STATISTICS from $OFFLINE_TABLE"))
      .union(spark.sql(s"select ${offlineNumColPercentile25.mkString(",")} , '25分位数' as off_$STATISTICS from $OFFLINE_TABLE"))
      .union(spark.sql(s"select ${offlineNumColPercentile50.mkString(",")} , '中位数' as off_$STATISTICS from $OFFLINE_TABLE"))
      .union(spark.sql(s"select ${offlineNumColPercentile75.mkString(",")} , '75分位数' as off_$STATISTICS from $OFFLINE_TABLE"))

    offlineCount.show()

    // 计算在线表字段的 均值、方差、25%分位数、中位数、75%分位数

    val onlineCount = spark.sql(s"select ${onlineNumColAvgExpr.mkString(",")} , '均值' as on_$STATISTICS from $ONLINE_TABLE")
      .union(spark.sql(s"select ${onlineNumColStd.mkString(",")} , '标准差' as on_$STATISTICS from $ONLINE_TABLE"))
      .union(spark.sql(s"select ${onlineNumColPercentile25.mkString(",")} , '25分位数' as on_$STATISTICS from $ONLINE_TABLE"))
      .union(spark.sql(s"select ${onlineNumColPercentile50.mkString(",")} , '中位数' as on_$STATISTICS from $ONLINE_TABLE"))
      .union(spark.sql(s"select ${onlineNumColPercentile75.mkString(",")} , '75分位数' as on_$STATISTICS from $ONLINE_TABLE"))

    onlineCount.show()

    // join两表 计算两表差值

    var countDIFF = offlineCount.join(onlineCount, offlineCount(s"off_$STATISTICS") === onlineCount(s"on_$STATISTICS"))

    for (col <- numCols) {
      val str = s"abs((on_${col.getOnlineField} - " +
        s"off_${col.getOfflineField} ) / (on_${col.getOnlineField} + off_${col.getOfflineField}) * 2 ) * 100"
      countDIFF = countDIFF.withColumn(s"diff_${col.getOfflineField}", expr(str))
    }

    val colSeq = collection.mutable.ArrayBuffer[String]()
    colSeq += s"off_${STATISTICS}"
    for (col <- numCols) {
      colSeq += s"diff_${col.getOfflineField}"
    }

    countDIFF = countDIFF.select(colSeq.head, colSeq.tail: _*)

    countDIFF.show()

    for (col <- numCols) {
      var value = 0.0

      if (!col.getAlarmFilter.isEmpty) {
        val filters = col.getAlarmFilter
        for (filter <- filters) {
          if ("PERCENT".equals(filter.getType)) {
            value = filter.getValue
          }
        }
      }

      // 数值统计结果

      val rows = countDIFF.filter(expr(s"diff_${col.getOfflineField} > $value"))
        .select(s"diff_${col.getOfflineField}", s"off_$STATISTICS")
        .selectExpr(s"collect_list(off_$STATISTICS)", s"collect_list(diff_${col.getOfflineField})")
        .collect()

      // 空值占比结果
      val (offNumNull,onNumNull)  = calCalculateNullResult(offlineDF, onlineDF, col)


      if (rows.length > 0) {
        val metrics = rows(0).getSeq[String](0)
        val percents = rows(0).getSeq[Double](1)
        var resultStr = collection.mutable.ArrayBuffer[String]()
        for (i <- metrics.indices) {
          val metric = metrics(i)
          val percent = percents(i).formatted("%.2f")
          resultStr += s"${metric}相差${percent}%"
        }

        resultStr += s"离线空值占比为${offNumNull}%,在线空值占比为${onNumNull}%"


        text += buildAlarmMessage(col.getOfflineField, col.getOnlineField, col.getType, value, resultStr.mkString(","))
      }

    }

    // 输出任务信息

    // 中断连接

    spark.stop()

  }

  /**
   * 计算标签类型的字段diff结果
   *
   * @param offlineDF 离线表
   * @param onlineDF  在线表
   * @param x         字段
   * @return diff结果的dataframe
   */
  private def calCalculateLabelDiffResult(offlineDF: DataFrame, onlineDF: DataFrame, x: Field) = {
    val offlineLB = offlineDF.groupBy(x.getOfflineField).count()
      .withColumn("off_percentage", format_number(col("count")
        .divide(sum("count").over)
        .multiply(100), 5))

    // 在线 标签类型的字段的占比
    val onlineLB = onlineDF.groupBy(x.getOnlineField).count()
      .withColumn("on_percentage", format_number(col("count")
        .divide(sum("count").over)
        .multiply(100), 5))

    // 合并 离线、在线 两个dataframe(全外连接)
    val joinDF = offlineLB.join(onlineLB, offlineLB(x.getOfflineField) === onlineLB(x.getOnlineField), "fullouter")

    // 计算离线、在线 同一字段的差值
    joinDF.withColumn("diff", expr("abs(nvl(off_percentage,0) - nvl(on_percentage,0))"))

  }

  /**
   * 计算所有类型的字段空值占比结果
   *
   * @param offlineDF 离线表
   * @param onlineDF  在线表
   * @param x         字段
   * @return 空值占比结果的dataframe
   */
  private def calCalculateNullResult(offlineDF: DataFrame, onlineDF: DataFrame, x: Field) = {

    val offlineNull = offlineDF
      .agg((sum(when(col(s"${x.getOfflineField}").isNotNull,0).otherwise(1))/count("*")).as("off_null"))
      .selectExpr( "round(off_null * 100, 2) as off_null")
      .collect()

    val onlineNull = onlineDF
      .agg((sum(when(col(s"${x.getOnlineField}").isNotNull,0).otherwise(1))/count("*")).as("on_null"))
      .selectExpr("round(on_null * 100 ,2) as on_null")
      .collect()

    (offlineNull(0).getDouble(0),onlineNull(0).getDouble(0))
  }

  def hasColumn(df: org.apache.spark.sql.DataFrame, colName: String) = df.columns.contains(colName)




}
*/

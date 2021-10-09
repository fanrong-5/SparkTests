package SparkSQL


import java.util

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonArray, JsonParser, JsonPrimitive}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Test_1
{
  def main(args: Array[String]): Unit =
  {

    if (args.length < 1)
    {
      print("Usage: 需要日期参数，格式：yyyy-MM-dd")
      System.exit(1)
    }

    val date = args(0)
    val limit = args(1)
    val webHook = args(2)
    val notifiedPerson = args(3)

    val spark = SparkSession.builder()
      .appName("Test_1")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.memory", "8g")
      .config("spark.driver.maxResultSize", "8g")
      .config("spark.sql.execution.arrow.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    // 获取表数据

    // todo 时间保持一致

    val aucSQL =
      """
        |SELECT DISTINCT(id),nvl(fin, 0) AS fin,字段3,字段4
        |FROM
        |     (SELECT
        |        get_json_object(param['字段3'],'$.concatenate') as 字段3,
        |        param['id'] as id
        |      FROM 表1
        |      WHERE concat_ws('-', year, month, day) = '""".stripMargin + date + """') a
        |INNER JOIN
        |      (SELECT
        |         id,
        |         字段2
        |      FROM 表2
        |      WHERE dt = '""".stripMargin + date + """'
        |      AND 字段2 BETWEEN 4 AND 10
        |      )  b
        |ON a.id = b.id
        |LEFT JOIN
        |     (SELECT id,fin
        |      FROM 表3
        |      WHERE dt = '""".stripMargin + date + """'
        |      and fin = 1 ) c
        |ON b.id = c.id
        |""".stripMargin


    val PREDICTION_TABLE = "prediction_table"

    var predictionTable = spark.sql(aucSQL)
    predictionTable.createOrReplaceTempView(PREDICTION_TABLE)

    println("-------------------------------------------------------表数据获取-------------------------------------------------------")
    predictionTable.show(false)

    println("---------------------------------------------循环遍历--------------------------------------------")

    import spark.implicits._

    val resultDF = predictionTable.map(row =>{
      val finish = row.getAs[Long]("字段1")     // long为字段1在原来表中的数据类型
      val coupon = row.getAs[Integer]("字段2")
      val listStr = row.getAs[String]("字段3")

      val index = 10 - coupon

      val gson = new Gson()
      val list: java.util.List[Double] = gson.fromJson(listStr, new TypeToken[java.util.List[Double]]()
      {}.getType())

      (finish,coupon, listStr,list.get(index))
    }
    ).toDF("字段1","字段2","字段3","字段4")

    println("---------------------------------------------最终展示--------------------------------------------")
    resultDF.show(false)

    // todo 选取特定的预测值

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("t")       // 真实值
      .setRawPredictionCol("p")   // 预测值
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(resultDF).formatted("%.6f")

    print(s"Test AUC: ${auc}")


  }

}

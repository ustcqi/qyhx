package com.aisino.bd.qyhx

import com.aisino.bd.common.SchemaUtil
import com.aisino.bd.qyhx.common.DateUtil
import com.aisino.bd.qyhx.math.MathUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kerwin on 16/9/28.
  * 周期性企业计算
  */
class ZQQY(context: AppContext) {
    val sqlContext = context.sqlContext
    val spark = context.spark

    val k = List(3, 6, 12)

    /**
     * 取数据时要全年的,例如一年,两年,某月没数据的填
     * @param nsrAyHzDF
     * @param startTime
     * @param endTime
     * @param k
     * @return
     */
    def zqNsr(nsrAyHzDF: DataFrame, startTime: String, endTime: String, k: Int, ratio: Double) : DataFrame = {
        val lirunDF = nsrAyHzDF.filter(s"ny >= ${startTime}").filter(s"ny <= ${endTime}").select("nsrsbh", "ny", "lr")
        val df = lirunDF.orderBy("ny").groupBy("nsrsbh").pivot("ny").mean("lr")
        val nsrCorrelationRDD = df.rdd.map(x => {
            val len = x.length - 1
            val arr = new Array[Double](len)
            for(i <- 1 to len){
                if(x(i) == null) arr(i-1) = 0.0
                else arr(i-1) = x(i).toString.toDouble
            }
            //k>=1 and  k<=n/2
            val r = MathUtils.autocorrelation(arr, k)
            Row(x(0), r)
        })

        val schema = StructType(
            StructField("nsrsbh", StringType, true)
                    :: StructField("coefficient", DoubleType, true)
                    :: Nil)
        val zqNsrDF = sqlContext.createDataFrame(nsrCorrelationRDD, schema)
                .toDF("nsrsbh", "coefficient")

        topZqNsrByRatio(zqNsrDF, ratio, endTime)
    }

    def topZqNsrByRatio(zqNsrDF: DataFrame, ratio: Double, endTime: String): DataFrame ={
        val convertedRDD = zqNsrDF.filter("coefficient > 0.0").rdd.map(x => {
            (x(0).toString, x(1).asInstanceOf[Double])
        })
        val zqNsrArr =  convertedRDD.collect().filter(! _._2.isNaN)
        val count = zqNsrArr.length
        val idx = (count * ratio).toInt
        val arr = zqNsrArr.sortBy(_._2).slice(count - idx, count)
        val currentTime = DateUtil.getCurrentTime()
        val rdd =  spark.sparkContext.parallelize(arr).map(x => Row(x._1.toString, 4.toString, 1, endTime, null, currentTime))
        val schema = SchemaUtil.nsrBqSchema
        val df = spark.createDataFrame(rdd, schema)
        df
    }
}

object ZQNSR{

    val usage =
		"""Usage: \n\t args(0): startTime \n\t args(1): endTime \n\t args(2): k \ n\t args(3) ratio

			 example: 201201 201312 12 0.2
		""".stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 4){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
        val k = args(2).toInt
        val ratio = args(3).toDouble

        val context = new AppContext()
        val dataLoader = new DataLoader(context)
        val nsrAyHzDF = dataLoader.getNsrAyHz()
        val zqqy = new ZQQY(context)

        val zqNsrDF = zqqy.zqNsr(nsrAyHzDF, startTime, endTime, k, ratio)
        zqNsrDF.write.mode("append").saveAsTable("dw_bak1.dw_dm_nsr_bq")

        context.sc.stop()
    }

    def test(zqNsrDF: DataFrame): Unit ={
        val efficients = List(0.1, 0.2, 0.3, 0.4, 0.5)
        val countArr = new Array[Long](efficients.length)
        for(i <- 0 until  efficients.length){
            countArr(i) = zqNsrDF.filter(s"coefficient > ${efficients(i)}").count
            println(s"count of (coefficient > ${efficients(i)}}) is ${countArr(i)}")
        }
    }
}
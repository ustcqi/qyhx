package com.aisino.bd.qyhx

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
  * Created by kerwin on 16/9/28.
  * 周期性企业计算
  */
class ZQQY(context: AppContext) {
    val sqlContext = context.sqlContext
    val spark = context.spark

    val k = List(3, 6, 12)

    //取数据时要全年的,例如一年,两年,某月没数据的填充为0
    /**
      * 计算求得周期性企业
      * @param xxfpDF
      * @param jxfpDF
      * @param nsrDF
      * @return zqNsrDF
      */
    def zqNsr(xxfpDF: DataFrame, jxfpDF: DataFrame, nsrDF: DataFrame) : DataFrame = {
        val dataSummary = new DataSummary(context)
        val nsrAyHzDF = dataSummary.nsrAyHz(xxfpDF, jxfpDF, nsrDF)
        val lirunDF = nsrAyHzDF.select("nsrsbh", "ny", "lirun")
        /* | nsrsbh  ny         lirun |
        *    12e43   2016-01    23.0
        *
        *    - - - - -
        *   nsrsbh    ny        lirun
        *   123       2016-01    23.0
        *   123       2016-02    34.0
        * =>
        *   nsrsbh    2016-01    2016-02
        *   123       23.0       34.0
        *
        * */
        val df = lirunDF.orderBy("ny").groupBy("nsrsbh").pivot("ny").mean("lirun")

        //transfer a rdd row to DenseVector and then compute the autocorrelation
        val nsrCorrelationRDD = df.rdd.map(x => {
            val len = x.length - 1
            val arr = new Array[Double](len)
            for(i <- 1 to len){
                if(x(i) == null) arr(i-1) = 0.0
                else arr(i-1) = x(i).toString.toDouble
            }
            //k=1, k<n
            val r = ZQNSR.autocorrelation(arr, 1)
            Row(x(0), r)
        })

        val schema = StructType(
            StructField("nsrsbh", StringType, true)
                    :: StructField("coefficient", DoubleType, true)
                    :: Nil)
        val zqNsrDF = sqlContext.createDataFrame(nsrCorrelationRDD, schema)
                .toDF("nsrsbh", "coefficient")
        zqNsrDF
    }
}

object ZQNSR{

    /**
      * 求序列arr基于lag=k时的相关系数
      * @param arr
      * @param k
      * @return  correlation coefficient (Double)
      */
    def autocorrelation(arr: Array[Double], k: Int): Double ={
        val mean = arr.sum / arr.length
        var variance = 0.0
        arr.foreach(x => {
            variance += (x - mean) * (x - mean)
        })
        var translatedVar = 0.0
        val n = arr.length
        for(i <- 0 until (n - k)){
            translatedVar += (arr(i) - mean) * (arr(i+k) - mean)
        }
        val correlation = (translatedVar / variance).formatted("%.3f").toDouble
        correlation
    }

    def main(args: Array[String]) {
        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        val xxfpDF = dataLoader.getXXFPData()
        val jxfpDF = dataLoader.getJXFPData()
        val nsrDF = dataLoader.getNSRData()

        val zqqy = new ZQQY(context)
        val zqNsrDF = zqqy.zqNsr(xxfpDF, jxfpDF, nsrDF)
        zqNsrDF.printSchema()
        zqNsrDF.show()
        context.sc.stop()
    }
}
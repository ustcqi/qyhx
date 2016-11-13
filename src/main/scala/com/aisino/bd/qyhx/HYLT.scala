package com.aisino.bd.qyhx

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by kerwin on 16/9/26.
  * 行业龙头企业计算
  */
class HYLT(context: AppContext) extends Serializable{
    @transient
    val sqlContext = context.sqlContext
    @transient
    val spark = context.spark

    val n = 5

    def hyLtNsr(xxfpDF: DataFrame, jxfpDF: DataFrame, nsrDF: DataFrame): DataFrame ={
        val dataSummary = new DataSummary(context)

        val nsrAyHzDF = dataSummary.nsrAyHz(xxfpDF, jxfpDF, nsrDF)
        val hyAyHzDF = dataSummary.hyAyHz(xxfpDF, jxfpDF, nsrDF)
        nsrAyHzDF.createOrReplaceTempView("nsrAyHzT")
        hyAyHzDF.createOrReplaceTempView("hyAyHzT")

        val DF = sqlContext.sql("select n.hydm, n.nsrsbh, n.ny, n.xxje, n.jxje, h.hyyxx, " +
                "n.jxje, h.hyyjx " +
                "from nsrAyHzT n, hyAyHzT h where n.hydm = h.hydm and n.ny = h.ny")
        DF.createOrReplaceTempView("nsrhyT")

        val tmpDF = sqlContext.sql("select nsrsbh, hydm, sum(xxje) as zxxje, sum(jxje) as zjxje, " +
                "sum(hyyxx) as hyzxx, sum(hyyjx) as hyzjx " +
                "from nsrhyT group by nsrsbh, hydm")
        tmpDF.createOrReplaceTempView("nsrhyhzT")

        val hzDF = sqlContext.sql("select nsrsbh, hydm, (zxxje/hyzxx) as xxbl from nsrhyhzT")
        hzDF.createOrReplaceTempView("ltqyT")
        val rdd = hzDF.rdd.map(x => (x(1).toString, (x(2).toString.toDouble, x(0).toString)))

        import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
        val ltqyRDD = rdd.topByKey(n)(Ordering.by[(Double, String), Double](_._1))
                .map(x => Row(x._1, x._2(0)._2, x._2(0)._1))
        //ltqyRDD.take(100).foreach(println)
        //println(retRDD.map(x => x._1).count(), retRDD.map(x => x._1).distinct.count())

        val schema = StructType(
            StructField("hydm", StringType, true)
                    :: StructField("nsrsbh", StringType, true)
                    :: StructField("xxbl", DoubleType, true)
                    :: Nil)
        val ltqyDF = spark.createDataFrame(ltqyRDD, schema).toDF("hydm", "nsrsbh", "xxbl")
        ltqyDF.printSchema()
        ltqyDF.show()
        /**
          *
        val tmpDF2 = sqlContext.sql("select gf_nsrsbh, je from (select gf_nsrsbh, je,
        row_number() OVER (PARTITION BY gf_nsrsbh ORDER BY je DESC) rank from dw_fact_jxfp) tmp
        where rank<=3")
        */
        ltqyDF
    }
}

object HYLT{
    def main(args: Array[String]) {
        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        val xxfpDF = dataLoader.getXXFPData()
        val jxfpDF = dataLoader.getJXFPData()
        val nsrDF = dataLoader.getNSRData()
        /*
        val xxDF = dataLoader.getXxData(sqlContext)
        val jxDF = dataLoader.getJxData(sqlContext)
        val nsrDF = dataLoader.getNsrData(sqlContext)
        */
        val hylt = new HYLT(context)
        hylt.hyLtNsr(xxfpDF, jxfpDF, nsrDF)
        context.sc.stop()
    }
}

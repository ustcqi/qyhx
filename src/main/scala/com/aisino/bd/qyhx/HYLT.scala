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

    def hyLtNsr(nsrAyHzDF: DataFrame, hyAyHzDF: DataFrame): DataFrame ={
        nsrAyHzDF.createOrReplaceTempView("nsrAyHzT")
        hyAyHzDF.createOrReplaceTempView("hyAyHzT")

        val DF = sqlContext.sql("select n.hy_dm, n.nsrsbh, n.ny, n.xxje, n.jxje, h.xxje as hyxxje, " +
                "n.jxje, h.jxje as hyjxje " +
                "from nsrAyHzT n, hyAyHzT h where n.hy_dm = h.hy_dm and n.ny = h.ny")
        DF.createOrReplaceTempView("nsrhyT")
        //DF.show()

        val tmpDF = sqlContext.sql("select nsrsbh, hy_dm, sum(xxje) as zxxje, sum(jxje) as zjxje, " +
                "sum(hyxxje) as hyzxxje, sum(hyjxje) as hyzjxje " +
                "from nsrhyT group by nsrsbh, hy_dm")
        tmpDF.createOrReplaceTempView("nsrhyhzT")

        val hzDF = sqlContext.sql("select nsrsbh, hy_dm, (zxxje/hyzxxje) as xxbl from nsrhyhzT")
        //hzDF.show()
        //println("test dataframe.rdd", hzDF.rdd.count)
        //hzDF.rdd.take(5).foreach(println)

        val rdd = hzDF.rdd.map(x => (x(1).asInstanceOf[String], (x(2).asInstanceOf[Double], x(0).asInstanceOf[String])))
        println("test rdd", rdd.count)
        //rdd.take(5).foreach(println)
        //rdd.foreach(println)
        import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
        val ltqyRDD = rdd.topByKey(n)(Ordering.by[(Double, String), Double](_._1))
                .map(x => Row(x._1, x._2(0)._2, x._2(0)._1))

        //ltqyRDD.take(100).foreach(println)
        //println(retRDD.map(x => x._1).count(), retRDD.map(x => x._1).distinct.count())

        val schema = StructType(
            StructField("hy_dm", StringType, true)
                    :: StructField("nsrsbh", StringType, true)
                    :: StructField("xxbl", DoubleType, true)
                    :: Nil)
        val ltqyDF = spark.createDataFrame(ltqyRDD, schema).toDF("hy_dm", "nsrsbh", "xxbl")
        ltqyDF
          /*
        val tmpDF2 = sqlContext.sql("select gf_nsrsbh, je from (select gf_nsrsbh, je,
        row_number() OVER (PARTITION BY gf_nsrsbh ORDER BY je DESC) rank from dw_fact_jxfp) tmp
        where rank<=3")
        */

    }
}

object HYLT{
    def main(args: Array[String]) {
        val context = new AppContext()

        val dataLoader = new DataLoader(context)

        val nsrAyHzDF = dataLoader.getNsrAyHz()
        val hyAyHzDF = dataLoader.getHyAyHz()
        val hylt = new HYLT(context)
        val hyltDF = hylt.hyLtNsr(nsrAyHzDF, hyAyHzDF)
        hyltDF.show()
        context.sc.stop()
    }
}

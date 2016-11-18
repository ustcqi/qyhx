package com.aisino.bd.qyhx

import com.aisino.bd.common.SchemaUtil
import com.aisino.bd.qyhx.common.DateUtil
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by kerwin on 16/9/26.
  * 行业龙头企业计算
  */
class HYLT(context: AppContext) extends Serializable{
    @transient
    val spark = context.spark

    def hyLtNsr(nsrAyHzDF: DataFrame, hyAyHzDF: DataFrame, startTime: String, endTime: String,  n: Int): DataFrame ={
        nsrAyHzDF.filter(s"ny >= ${startTime}").filter(s"ny <= ${endTime}").createOrReplaceTempView("nsrAyHzT")
        hyAyHzDF.filter(s"ny >= ${startTime}").filter(s"ny <= ${endTime}").createOrReplaceTempView("hyAyHzT")

        val DF = spark.sql("select n.hy_dm, n.nsrsbh, n.ny, n.xxje, n.jxje, h.xxje as hyxxje, " +
            "n.jxje, h.jxje as hyjxje " +
            "from nsrAyHzT n, hyAyHzT h where n.hy_dm = h.hy_dm and n.ny = h.ny")
        DF.createOrReplaceTempView("nsrhyT")
        //DF.show()

        val tmpDF = spark.sql("select nsrsbh, hy_dm, sum(xxje) as zxxje, sum(jxje) as zjxje, " +
            "sum(hyxxje) as hyzxxje, sum(hyjxje) as hyzjxje " +
            "from nsrhyT group by nsrsbh, hy_dm")
        tmpDF.createOrReplaceTempView("nsrhyhzT")

        val hzDF = spark.sql("select nsrsbh, hy_dm, (zxxje/hyzxxje) as xxbl from nsrhyhzT")

        val rdd = hzDF.rdd.map(x => (x(1).asInstanceOf[String], (x(2).asInstanceOf[Double], x(0).asInstanceOf[String])))

        import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
        /*
        val schema = StructType(
            StructField("hy_dm", StringType, true)
                :: StructField("nsrsbh", StringType, false)
                :: StructField("xxbl", DoubleType, true)
                :: Nil)
        val ltqyRDD = rdd.topByKey(n)(Ordering.by[(Double, String), Double](_._1))
            .map(x => Row(x._1, x._2(0)._2, x._2(0)._1))
        */
        val currentTime = DateUtil.getCurrentTime()
        val ltqyRDD = rdd.topByKey(n)(Ordering.by[(Double, String), Double](_._1))
            .map(x => Row(x._2(0)._2.toString, 6.toString, 1, endTime, null, currentTime))

        val schema = SchemaUtil.nsrBqSchema
        val ltqyDF = spark.createDataFrame(ltqyRDD, schema)
        ltqyDF
    }
}

object HYLT{
    val usage =
        """Usage:
	            args(0): startTime
			    args(1): endTime
                args(2): n
			 example:
	            201201 201312 5
        """.stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 2){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
        val n = args(2).toInt

        val context = new AppContext()

        val dataLoader = new DataLoader(context)

        val nsrAyHzDF = dataLoader.getNsrAyHz()
        val hyAyHzDF = dataLoader.getHyAyHz()
        val hylt = new HYLT(context)
        //val hyltDF = hylt.hyLtNsr(nsrAyHzDF, hyAyHzDF)
        val hyltDF = hylt.hyLtNsr(nsrAyHzDF, hyAyHzDF, startTime, endTime, n)
        hyltDF.write.mode("append").saveAsTable("dw_bak1.dw_dm_nsr_bq")
        //hyltDF.show()
        context.sc.stop()
    }
}

package com.aisino.bd.qyhx

import com.aisino.bd.common.SchemaUtil
import com.aisino.bd.qyhx.common.DateUtil
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by kerwin on 16/9/28.
  * 外贸型企业计算
  */
class WMQY(context: AppContext) {
    val spark = context.spark
    import spark.implicits._

    def wmNsr(nsrDF: DataFrame, startTime: String, endTime: String) : DataFrame ={
        val currentTime = DateUtil.getCurrentTime()
        val nsrRDD = nsrDF.select("nsrsbh")
            .rdd.map(x => Row(x(0).toString, 1.toString, 1, endTime, null, currentTime))
        val schema = SchemaUtil.nsrBqSchema
        val ltqyDF = spark.createDataFrame(nsrRDD, schema)
        ltqyDF
    }
}

object WMQY{
    val usage =
        """Usage:
	            args(0): startTime
			    args(1): endTime

			 example:
	            201201 201312
        """.stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 2){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)

        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        //val nsrDF = dataLoader.getWmNSRData()
        val nsrDF = dataLoader.getWmNsrData()
        val wm = new WMQY(context)
        val wmNsrDF = wm.wmNsr(nsrDF, startTime, endTime)
        wmNsrDF.write.mode("append").saveAsTable("dw_bak1.dw_dm_nsr_bq")
        wmNsrDF.rdd.foreach(println)
        context.sc.stop()
    }
}
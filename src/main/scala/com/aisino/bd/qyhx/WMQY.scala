package com.aisino.bd.qyhx

import com.aisino.bd.common.AppContext
import com.aisino.bd.Utils.{SchemaUtil, DateUtil}
import com.aisino.bd.common.{DataLoader}
import org.apache.spark.sql.{Row, DataFrame}

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
            .rdd.map(x => Row(x(0).toString, 1.toString, 1.toDouble, endTime, null, currentTime))
        val schema = SchemaUtil.nsrBqSchema
        val wmNsrDF = spark.createDataFrame(nsrRDD, schema)
        wmNsrDF
    }
}

object WMQY{
    val usage =
        """Usage:
	            args(0): startTime
			    args(1): endTime
                args(2): table
			 example:
	            201201 201312 dw_bak1.dw_dm_nsr_bq1
        """.stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 3){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
        val tableName = args(2)

        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        //val nsrDF = dataLoader.getWmNSRData()
        val nsrDF = dataLoader.getWmNsrData()
        val wm = new WMQY(context)
        val wmNsrDF = wm.wmNsr(nsrDF, startTime, endTime)
        wmNsrDF.write.mode("append").saveAsTable(tableName)
        wmNsrDF.rdd.foreach(println)
        context.sc.stop()
    }
}
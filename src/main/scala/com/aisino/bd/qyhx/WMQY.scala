package com.aisino.bd.qyhx

import org.apache.spark.sql.DataFrame
/**
  * Created by kerwin on 16/9/28.
  * 外贸型企业计算
  */
class WMQY(context: AppContext) {
    val spark = context.spark
    import spark.implicits._

    def wmNsr(nsrDF: DataFrame): Array[String] ={
        val nsrArr = nsrDF.select("nsrsbh")
                .map(x => x(0).toString)
                .collect()
        nsrArr
    }
}

object WMQY{
    def main(args: Array[String]) {
        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        //val nsrDF = dataLoader.getWmNSRData()
        val nsrDF = dataLoader.getWmNsrData()
        val wm = new WMQY(context)
        val nsrArr = wm.wmNsr(nsrDF)
        nsrArr.foreach(println)
        context.sc.stop()
    }
}
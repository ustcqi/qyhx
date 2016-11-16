package com.aisino.bd.qyhx

import org.apache.spark.sql.DataFrame
/**
  * Created by kerwin on 16/9/23.
  */
class DataSummary(context: AppContext){
    val sqlContext = context.sqlContext

    /**
      * 根据 (nsrsbh + date)确定进项,销项的总额然后存储到企业按月汇总表中
      * 1.计算进项
      * 2.计算销项
      * 3.根据进项,销项计算利润
      * 4.完成nsr_ay_hz表
      *
      * @param xxfpDF , nsrDF
      */
    def nsrAyHz(xxfpDF: DataFrame, jxfpDF: DataFrame, nsrDF: DataFrame): DataFrame = {
        val xxfpNsrDF = xxfpDF.join(nsrDF, xxfpDF("xf_nsrsbh") === nsrDF("nsrsbh"), "inner")
                .filter("hy_dm != ''")
        val jxfpNsrDF = jxfpDF.join(nsrDF, jxfpDF("gf_nsrsbh") === nsrDF("nsrsbh"), "inner")
                .filter("hy_dm != ''")

        val xxDF = xxfpNsrDF.groupBy("nsrsbh", "hy_dm", "kprq")
                .sum("je")
                .toDF("nsrsbh", "hy_dm", "ny", "xxyje")
        val jxDF = jxfpNsrDF.groupBy("nsrsbh", "hy_dm", "kprq")
                .sum("je")
                .toDF("nsrsbh", "hy_dm", "ny", "jxyje")
        xxDF.createOrReplaceTempView("xxtmpT")
        jxDF.createOrReplaceTempView("jxtmpT")

        val nsrAyHzDF = sqlContext.sql(
            "select  x.nsrsbh, x.hy_dm , x.ny, x.xxyje as xxje, j.jxyje as jxje, " +
                    "(x.xxyje - j.jxyje) as lirun " +
                    "from xxtmpT x, jxtmpT j where x.nsrsbh = j.nsrsbh and x.ny = j.ny")
        nsrAyHzDF
    }

    /**
      * 行业按月汇总表
      * @param xxfpDF
      * @param jxfpDF
      * @param nsrDF
      */
    def hyAyHz(xxfpDF: DataFrame, jxfpDF: DataFrame, nsrDF: DataFrame): DataFrame = {
        val ayhzDF = nsrAyHz(xxfpDF, jxfpDF, nsrDF)
        ayhzDF.createOrReplaceTempView("nsrAyHzT")
        val hyAyHzDF = sqlContext.sql("select hy_dm, ny, sum(xxje) as hyyxx, sum(jxje) as hyyjx " +
                "from nsrAyHzT group by hy_dm, ny order by ny")
        hyAyHzDF
    }

    /*
       val xxfpNsrRestDF = xxfpDF.join(nsrDF, xxfpDF("xf_nsrsbh") === nsrDF("nsrsbh"), "inner")
               .filter("hy_dm = ''")
       val jxfpNsrRestDF = jxfpDF.join(nsrDF, jxfpDF("gf_nsrsbh") === nsrDF("nsrsbh"), "inner")
               .filter("hy_dm = ''")
       val xxRestDF = xxfpNsrRestDF.groupBy("xf_nsrsbh", "gf_nsrsbh", "kprq")
               .sum("je")
               .toDF("xf_nsrsbh", "gf_nsrsbh", "hy_dm", "ny", "xxyje")
       val jxRestDF = jxfpNsrRestDF.groupBy("nsrsbh", "kprq")
               .sum("je")
               .toDF("gf_nsrsbh", "xf_nsrsbh", "hy_dm", "ny", "jxyje")

       */
    /**
      * nsr <-> nsr 利润 进项 销项 按月汇总表
      * @param xxfpDF
      * @param jxfpDF
      * @param nsrDF
      */
    def nsrToNsrAyHz(xxfpDF: DataFrame, jxfpDF: DataFrame, nsrDF: DataFrame): DataFrame ={
        val xxfpNsrDF = xxfpDF.join(nsrDF, xxfpDF("xf_nsrsbh") === nsrDF("nsrsbh"), "inner")
                .filter("hy_dm != ''")
        val jxfpNsrDF = jxfpDF.join(nsrDF, jxfpDF("gf_nsrsbh") === nsrDF("nsrsbh"), "inner")
                .filter("hy_dm != ''")
        val xxDF = xxfpNsrDF.groupBy("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "kprq")
                .sum("je")
                .toDF("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "ny", "xxyje")
        val jxDF = jxfpNsrDF.groupBy("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "kprq")
                .sum("je")
                .toDF("hy_dm", "gf_nsrsbh", "xf_nsrsbh", "ny", "jxyje")
        //xxDF.printSchema()
        xxDF
    }

    def nsrToNsrHz(df: DataFrame): DataFrame = {
        val anHzDF = df.groupBy("hy_dm", "xf_nsrsbh", "gf_nsrsbh")
                .sum("xxyje")
                .toDF("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "xxnje")
        //anHzDF.printSchema()
        //anHzDF.show()
        anHzDF
    }
}

package com.aisino.bd.common

import org.apache.spark.sql.DataFrame

/**
  * Created by kerwin on 16/9/20.
  */
class DataLoader(context: AppContext){
    val spark = context.spark

    val hiveDatabase = "dw_bak1"
    val nsrInfoTable = "dw.dw_hbase_nsr_ei"

    def dropTable(tableName: String): Unit = {
        spark.sql(s"drop table $tableName")
    }

    //sqlContext.udf.register("parseKPRQ", (line : String) => parseDate(line))

    def xxfp(df: DataFrame): DataFrame ={
        val filterDF = df.select(df("xf_nsrsbh"), df("gf_nsrsbh"), df("je"), df("kprq"))
        filterDF
    }

    def jxfp(df: DataFrame): DataFrame = {
        val filterDF = df.select(df("xf_nsrsbh"), df("gf_nsrsbh"), df("je"), df("kprq"))
        //filterDF.printSchema()
        val kprq = filterDF.select("kprq")
        filterDF
    }

    def nsr(df: DataFrame): DataFrame = {
        val filterDF = df.select(df("nsrsbh"), df("hy_dm"))
        filterDF
    }

    def transfer(s: String) : String ={
        val ss = s.split(" ")(0)
        ss
    }

    def getJxData(): DataFrame = {
        val jxDF = spark.sql("select gf_nsrsbh, xf_nsrsbh, je, kprq from dw_bak1.dw_fact_jxfp")
        jxDF
    }

    def getXxData(): DataFrame = {
        val xxDF = spark.sql("select gf_nsrsbh, xf_nsrsbh, je, kprq from dw_bak1.dw_fact_xxfp")
        xxDF
    }

    def getNsrData(): DataFrame = {
        //val nsrDF = spark.sql("select rowkey as nsrsbh, nsrxx['hy_dm'] as hy_dm from dw.dw_hbase_nsr_ei")
        val nsrDF = spark.sql("select rowkey as nsrsbh, nsrkz['hymx_dm'] as hy_dm from dw.dw_hbase_nsr_ei where nsrkz['hymx_dm'] = ''")
        nsrDF
    }

    def getWmNsrData(): DataFrame = {
        val wmNsrDF = spark.sql("select rowkey as nsrsbh from dw.dw_hbase_nsr_ei where ckts['nsrdj_no'] != '' ")
        wmNsrDF
    }

    def getNsrAyHz() : DataFrame = {
        val nsrAyHzDF = spark.sql("select hy_key as hy_dm, nsr_key as nsrsbh, date_key as ny, lr, jxje, xxje from dw_bak1.dw_agg_hy_nsr")
        nsrAyHzDF
    }

    def getHyAyHz() : DataFrame = {
        val hyAyHzDF = spark.sql("select hy_key as hy_dm, date_key as ny, lr, jxje, xxje from dw_bak1.dw_agg_hy")
        hyAyHzDF
    }

    def getXfToGfNsrAyDF(): DataFrame = {
        val xfToGfNsrDF = spark.sql("select hy_key as hy_dm, nsr_key as xf_nsrsbh, jy_nsrsbh as gf_nsrsbh, date_key as ny, je from dw_bak1.dw_agg_nsr_sxygx")
        xfToGfNsrDF
    }

    def getXfToGfNsrAyDF(startTime: String, endTime: String) : DataFrame = {
        val xfToGfNsrAyDF = spark.sql(s"select hy_key as hy_dm, nsr_key as xf_nsrsbh, jy_nsrsbh as gf_nsrsbh, date_key as ny, je " +
            s"from dw_bak1.dw_agg_nsr_sxygx where date_key >= $startTime and date_key <= $endTime")
        xfToGfNsrAyDF
    }

    def getXfToGfNsrDF(startTime: String, endTime: String) : DataFrame = {
        val xfToGfNsrAyDF = getXfToGfNsrAyDF(startTime, endTime)
        xfToGfNsrAyDF.groupBy("hy_dm", "xf_nsrsbh", "gf_nsrsbh").sum("je").toDF("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "zje")
    }
}
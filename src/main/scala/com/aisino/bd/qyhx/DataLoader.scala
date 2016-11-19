package com.aisino.bd.qyhx

import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame

/**
  * Created by kerwin on 16/9/20.
  */
class DataLoader(context: AppContext){
    val spark = context.spark
    val sqlContext = context.sqlContext

    val nsrFile = "./data/nsr/nsr.parquet"
    val xxfpFile = "./data/xxfp/xxfp.parquet"
    val jxfpFile = "./data/jxfp/jxfp.parquet"
    val wmNsrFile = "./data/nsrwm/nsr.parquet"

    //sqlContext.udf.register("parseKPRQ", (line : String) => parseDate(line))

    def xxfp(df: DataFrame): DataFrame ={
        val filterDF = df.select(df("xf_nsrsbh"), df("gf_nsrsbh"), df("je"), df("kprq"))
        filterDF
    }


    def jxfp(df: DataFrame): DataFrame = {
        val filterDF = df.select(df("xf_nsrsbh"), df("gf_nsrsbh"), df("je"), df("kprq"))
        filterDF.printSchema()
        val kprq = filterDF.select("kprq")
        filterDF
    }

    def nsr(df: DataFrame): DataFrame = {
        val filterDF = df.select(df("nsrsbh"), df("hy_dm"))
        //filterDF.saveAsTable("test")
        filterDF
    }

    def parseDate(s: String): String ={
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        val ym = new SimpleDateFormat("yyyyMM")
        ym.format(sdf.parse(s))
    }

    def transfer(s: String) : String ={
        val ss = s.split(" ")(0)
        ss
    }

    def getJXFPData() : DataFrame ={
        val jxfpDF = sqlContext.read.load(jxfpFile)
        jxfpDF
    }

    def getXXFPData() : DataFrame ={
        val xxfpDF = sqlContext.read.load(xxfpFile)
        xxfpDF
    }

    def getNSRData(): DataFrame ={
        val nsrDF = sqlContext.read.load(nsrFile)
        nsrDF
    }

    def getWmNSRData(): DataFrame = {
        val nsrDF = sqlContext.read.load(wmNsrFile)
        nsrDF
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
}
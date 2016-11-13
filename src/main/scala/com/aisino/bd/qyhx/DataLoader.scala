package com.aisino.bd.qyhx

import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame

/**
  * Created by kerwin on 16/9/20.
  */
class DataLoader(context: AppContext){
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
        val jxDF = sqlContext.sql("select gf_nsrsbh, xf_nsrsbh, je, kprq from dw_bak.dw_fact_jxfp")
        jxDF
    }

    def getXxData(): DataFrame = {
        val xxDF = sqlContext.sql("select gf_nsrsbh, xf_nsrsbh, je, kprq from dw_bak.dw_fact_xxfp")
        xxDF
    }

    def getNsrData(): DataFrame = {
        val nsrDF = sqlContext.sql("select rowkey as nsrsbh, nsrxx['hy_dm'] as hydm from dw.dw_hbase_nsr")
        nsrDF
    }

    def getWmNsrData(): DataFrame = {
        val wmNsrDF = sqlContext.sql("select rowkey as nsrsbh, nsrxx['hy_dm'] as hydm from dw.dw_hbase_nsr where ckts['nsrdj_no'] != ''")
        wmNsrDF
    }
}
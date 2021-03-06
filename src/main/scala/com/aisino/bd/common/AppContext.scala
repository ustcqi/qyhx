package com.aisino.bd.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kerwin on 16/9/27.
  */
class AppContext {
    val appName = "qyhxApp"

    val conf = new SparkConf()
            .setAppName(appName)
            //.set("spark.executor.memory", "2g")
            //.set("spark.worker.cores", "1")
            //.setMaster("local")
    val sc = new SparkContext(conf)

    //val sqlContext = new SQLContext(sc) //spark-2.0 deprecated

    val spark = SparkSession.builder()
            .appName(appName)
            .enableHiveSupport()
            //.config("spark.some.config.option", "some-value")
            .config("spark.sql.pivotMaxValues", "10000")
            //.master("local")
            .getOrCreate()

    val sqlContext = spark.sqlContext
}

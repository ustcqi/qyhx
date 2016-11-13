package com.aisino.bd.qyhx

import org.apache.spark.{SparkConf, SparkContext}

class Test{
    def func(): Unit ={
        println("hello world")
    }
}

object Test{
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)
        println(distData.count)
    }
}
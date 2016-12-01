package com.aisino.bd.qyhx

import com.aisino.bd.common.AppContext
import com.aisino.bd.Utils.{SchemaUtil, DateUtil}
import com.aisino.bd.common.{DataLoader}

import breeze.numerics.{abs, pow}
import com.aisino.bd.qyhx.mathUtil.MathUtils

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, DataFrame}

/**
  * Created by kerwin on 16/9/26.
  * 快速增长企业计算
  * 1.以月为单位计算企业利润/销项总额
  * 2.取最近的6/12个月的利润/销项作为训练数据,6/12作为参数传入
  * (取最近的几个月的数学需要单独的函数做提取)
  * 3.用Linear  Regression
  * 4.取模型参数
  * 5.ratio:
  *    coefficient > 0.1  => top 1% level = 5, 1% ~ 2% level = 4, 2% ~ 3% level = 3, 3% ~ 4% level = 2, 4% ~ 5% level = 1
  *    abs(coefficient) < 0.1 => stable
  *    coefficient < -0.1 => top 1% level = -5, 1% ~ 2% level = -4, 2% ~ 3% level = -3, 3% ~ 4% level = -2, 4% ~ 5% level = -1
  *   top:
  *    coefficient > 0.0, top 10 level=5, top 20 ~ top 10 level = 4, ...
  *    abs(coefficient) <= 0.1  top minimal 10
  *    coefficient < -0.1, top 10 level=-5, top 20 ~ top 10 level = -4,
  */
class KSZZ(context: AppContext) extends Serializable{
    @transient
    val sqlContext = context.sqlContext
    val spark = context.spark

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Any]


    def getTrainData(arr: Array[Any]): List[(Double, DenseVector)] = {
        var seq = List[(Double, DenseVector)]()
        val (minValue, maxValue) = MathUtils.getMinMaxOfArray(arr)
        val scale = pow(10, MathUtils.getNumDigits(minValue))
        for(i <- 0 until arr.length){
            val lirun = arr(i).asInstanceOf[Double]
            val tmpSeq = List((lirun.formatted("%.3f").toDouble/scale,  new DenseVector(Array(i))))
            seq = seq ++ tmpSeq
        }
        seq
    }

    def train(arr: Array[Any]) : Double = {
        val seq = getTrainData(arr)
        val trainingDataDF = sqlContext.createDataFrame(seq).toDF("label", "features")
        val lr = new LinearRegression()
            .setMaxIter(1000)
            .setRegParam(0.2)
            .setLabelCol("label")
            .setFeaturesCol("features")
        val lrModel = lr.fit(trainingDataDF)
        val coefficients = lrModel.coefficients
        val w = coefficients(0)
        w.formatted("%.3f").toDouble
    }

    /**
     * 返回值是一个DF,两列,nsrsbh  level,分别表示纳税人识别号和增长的指标
     *  汇总时日期精确到天,然后groupby按月,sum(lirun)即可 ,备选方案
     *  某月利润为null时填充为0,不然报错
     *  设置不同的接口:
     *     kszzNsr(nsrAyHzDF: DataFrame, startTime: String, numOfMonth: Int) : DataFrame
     *     kszznsr(nsrAyHzDF: DataFrame, deadline: String, numOfMonth: Int) : DataFrame
     * @param nsrAyHzDF
     * @param startTime
     * @param endTime
     * @param ratio
     * @return
     */

    def kszzNsr(nsrAyHzDF: DataFrame, startTime: String, endTime: String, ratio: Double = 1.0) : DataFrame = {
        val lirunDF =  nsrAyHzDF.filter(s"ny >= ${startTime}").filter(s"ny <= ${endTime}").select("nsrsbh", "ny", "xxje")
        val monthNum = lirunDF.select("ny").distinct.count.toDouble.toInt
        val nsrHzDF = lirunDF.orderBy("ny").groupBy("nsrsbh").pivot("ny").mean("xxje")
		var nsrList = List[(String, Double)]()
        nsrHzDF.collect().foreach(x => {
            val nsrsbh = x(0).toString
            val arr =  x.toSeq.toArray.slice(1, monthNum + 1)

            if(! arr.contains(null)){
                val w = train(arr)
                //val level = kszzCheck(w)
                nsrList = nsrList :+ (nsrsbh, w)
            }
        })
		nsrList = nsrList.sortBy(_._2).filter(_._2 > 0.0)
        val len = nsrList.length
        val idx = (len * ratio).toInt
		if(len > 1) nsrList = nsrList.slice(len - idx - 1, len)
        val currentTime = DateUtil.getCurrentTime()
        val rdd =  spark.sparkContext.parallelize(nsrList).map(x => Row(x._1.toString, 2.toString, x._2.toDouble, endTime, null, currentTime))
        val schema = SchemaUtil.nsrBqSchema
        val df = spark.createDataFrame(rdd, schema)
        df
    }
}

object  KSZZ{
    val usage =
        """Usage:
	            args(0): startTime
			    args(1): endTime
                args(2): ratio
				args(3): table
			 example:
	            201201 201312 0.2 dw_bak1.dw_dm_nsr_bq1
        """.stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 4){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
        val ratio = args(2).toDouble
        val tableName = args(3)

        val context = new AppContext()
        val dataLoader = new DataLoader(context)
        val nsrAyHzDF = dataLoader.getNsrAyHz()
        val kszz = new KSZZ(context)
        val df = kszz.kszzNsr(nsrAyHzDF, startTime, endTime, ratio)
        df.write.mode("append").saveAsTable(tableName)
        context.sc.stop()
    }
}

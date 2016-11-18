package com.aisino.bd.qyhx

import com.aisino.bd.common.SchemaUtil
import com.aisino.bd.qyhx.common.DateUtil
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
  */
class KSZZ(context: AppContext) extends Serializable{
    @transient
    val sqlContext = context.sqlContext
    val spark = context.spark
    import spark.implicits._

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Any]


    def getTrainData(arr: Array[Any], n: Int): List[(Int, DenseVector)] = {
        var seq = List((1, new DenseVector(Array(1.0))))
        for(i <- 2 until (arr.length+1)){
            val lirun = arr(i-1).asInstanceOf[Double]
            val tmpSeq = List((i,  new DenseVector(Array(lirun))))
            seq = seq ++ tmpSeq
        }
        //seq = seq.slice(1, seq.length)
        seq
    }

    def train(arr: Array[Any], n: Int) : Double = {
        val seq = getTrainData(arr, n)
        val trainingDataDF = sqlContext.createDataFrame(seq).toDF("label", "features")
        val lr = new LinearRegression().setMaxIter(1000).setRegParam(0.05).setLabelCol("label").setFeaturesCol("features")
        val lrModel = lr.fit(trainingDataDF)
        val coefficients = lrModel.coefficients
        val w = coefficients(0)
        w.formatted("%.8f").toDouble
    }

    //根据实验结果调整
    def kszzCheck(w: Double) : Int = {
        if(w >= 0.5 & w < 1) 1
        else if(w>= 1 & w < 2) 2
        else if(w >= 2) 3
        else if(w >= 0.0 & w < 0.5) 0
        else if(w <0.0 & w > -0.5) 0
        else -1
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
        var nsrList = List(("head", 0.0))
        nsrHzDF.collect().foreach(x => {
            val nsrsbh = x(0).toString
            val arr =  x.toSeq.toArray.slice(1, monthNum + 1)
            if(! arr.contains(null)){
                val w = train(arr, monthNum)
                val level = kszzCheck(w)
                nsrList = nsrList :+ (nsrsbh, w)
            }
        })
        val len = nsrList.length-1
        val idx = (len * ratio).toInt
        nsrList = nsrList.slice(1, nsrList.length).sortBy(_._2).slice(len-idx, len)
        val kszzNsrDF = sqlContext.createDataFrame(nsrList).toDF("nsrsbh", "level").filter("level >= 0.0")
        val currentTime = DateUtil.getCurrentTime()
        val rdd =  spark.sparkContext.parallelize(nsrList).map(x => Row(x._1.toString, 2.toString, 1, endTime, null, currentTime))
        val schema = SchemaUtil.nsrBqSchema
        val df = spark.createDataFrame(rdd, schema)
        kszzNsrDF
    }
}

object  KSZZ{
    val usage =
        """Usage:
	            args(0): startTime
			    args(1): endTime

			 example:
	            201201 201312
        """.stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 3){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
        val ratio = args(2).toDouble
        val context = new AppContext()
        val dataLoader = new DataLoader(context)
        val nsrAyHzDF = dataLoader.getNsrAyHz()
        val kszz = new KSZZ(context)
        val df = kszz.kszzNsr(nsrAyHzDF, startTime, endTime, ratio)
        df.write.mode("append").saveAsTable("dw_bak1.dw_dm_nsr_bq")
        //df.write.mode("append").saveAsTable("dw_bak1.dw_dm_nsr_bq")
        //df.show()
        //df.orderBy("level").collect().foreach(println)
        context.sc.stop()
    }
}

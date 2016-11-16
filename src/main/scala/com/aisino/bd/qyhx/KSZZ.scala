package com.aisino.bd.qyhx

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame

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
     * @return
     */
    def kszzNsr(nsrAyHzDF: DataFrame, startTime: String, endTime: String) : DataFrame = {
        //filter the nsr which lirun is not null
        val lirunDF =  nsrAyHzDF.filter(s"ny >= ${startTime}").filter(s"ny <= ${endTime}").select("nsrsbh", "ny", "lr")
        val monthNum = lirunDF.select("ny").distinct.count.toDouble.toInt
        val nsrHzDF = lirunDF.orderBy("ny").groupBy("nsrsbh").pivot("ny").mean("lr")
        var nsrList = List(("tail", 0))
        val nsrRDD = lirunDF.select("nsrsbh").map(x => x(0).asInstanceOf[String]).distinct
        nsrRDD.take(2).foreach(x => {
            val nsrLrDF = nsrHzDF.filter(s"nsrsbh == ${x}")
            //nsrLrDF.show()
            val (nsrsbh, w) = train(nsrLrDF, monthNum)
            val level = kszzCheck(w)
            //println("coefficient is : " + w + " and level is " + level)
            nsrList = nsrList :+ (nsrsbh, level)
        })
        nsrList = nsrList.slice(1, nsrList.length)
        val kszzNsrDF = sqlContext.createDataFrame(nsrList).toDF("nsrsbh", "level")
        //kszzNsrDF.show
        kszzNsrDF
    }


    /**
      * @param df
      * @param n
      * @return List[(Int, Double)]
      */
    def dataPreprocess(df: DataFrame, n: Int): List[(Int, DenseVector)] = {
        val arr = new Array[Double](n+1)
        df.collect().foreach(x => {
            for(i <- 1 to n){
                if(x(i) == null) arr(i) = 0.0
                else arr(i) = x(i).asInstanceOf[Double]
            }
        })
        var seq = List((0, new DenseVector(Array(0.0))))
        for(i <- 1 until arr.length){
            val lirun = arr(i)
            val tmpSeq = List((i,  new DenseVector(Array(lirun))))
            seq = seq ++ tmpSeq
        }
        seq = seq.slice(1, seq.length)
        seq
    }

    /**
      * 获取DF中纳税人的月份数
      * @param df
      * @return
      */
    def getMonthNum(df: DataFrame) : Int = {
        val len = df.columns.length
        //去掉纳税人一列,剩下的是月份数
        len - 1
    }

    /**
      * 假设df的月份固定,设为12.
      * +---------------+------------------+------+
        |         nsrsbh|            201511|201512|
        +---------------+------------------+------+
        |370102307138004|261782.09000000003|  null|
        +---------------+------------------+------+

        则将df转为12行的df,nsrsbh=370102307138004,例如
        +---------------+------------------+------+
        |            201511|261782.09000000003|
        +---------------+------------------+------+
        |            201512|  null|
        +---------------+------------------+

        又由于df中月份是连续的(根据实际情况分析,可以验证下),因此可以忽略月份维度
        建立模型 y = w*x,其中y是利润,x是月份,w是权重,求得的w即为参数,也就是斜率
      */
    def train(df: DataFrame, n: Int) : (String, Double) = {
        val nsrsbh = df.select("nsrsbh").toString
        val seq = dataPreprocess(df, n)
        val trainingDataDF = sqlContext.createDataFrame(seq).toDF("label", "features")
        //trainingDataDF.show()
        val lr = new LinearRegression().setMaxIter(10).setRegParam(0.2).setLabelCol("label").setFeaturesCol("features")
        val lrModel = lr.fit(trainingDataDF)
        val coefficients = lrModel.coefficients
        val w = coefficients(0)
        (nsrsbh, w)
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
}

object  KSZZ{
    def main(args: Array[String]) {
        val context = new AppContext()
        val dataLoader = new DataLoader(context)
        val nsrAyHzDF = dataLoader.getNsrAyHz()
        val kszz = new KSZZ(context)
        val startTime = "201201"
        val endTime = "201312"
        val df = kszz.kszzNsr(nsrAyHzDF, startTime, endTime)
        context.sc.stop()
    }
}

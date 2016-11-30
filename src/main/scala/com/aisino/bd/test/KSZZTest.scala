package com.aisino.bd.test

import breeze.numerics._
import com.aisino.bd.common.DataLoader
import com.aisino.bd.common.AppContext
import com.aisino.bd.qyhx.math.MathUtils
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegression

/**
 * Created by kerwin on 16-11-18.
 */
object KSZZTest {
	val context = new AppContext()
	val sqlContext = context.sqlContext


	def main(args: Array[String]) {


		val w = kszzNsr()
		println(w)
		context.sc.stop()
	}

	def kszzNsr() : Double = {
		val arr = Array(5496536.0, 200895, 1268418, 401365,  3690017, 1035349, 18183972, 7001843,	6463026, 16613952, 9170859, 168213522)
		val coefficient = train(arr)
		//println(coefficient)
		coefficient
	}

	def train(arr: Array[Double]) : Double = {
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

	def getTrainData(arr: Array[Double]): List[(Double, DenseVector)] = {
		var seq = List((0.0, new DenseVector(Array(1.0))))
		val (minValue, maxValue) = getMinMaxOfArray(arr)
		val scale = pow(10, MathUtils.getNumDigits(minValue))
		for(i <- 1 until (arr.length)){
			val lirun = arr(i).asInstanceOf[Double]
			val tmpSeq = List((lirun.formatted("%.3f").toDouble/scale,  new DenseVector(Array(i))))
			seq = seq ++ tmpSeq
		}
		seq.slice(1, seq.length)
	}

	def getMinMaxOfArray(arr: Array[Double]) : (Double, Double) = {
		if(arr.length == 0){
			(0.0, 0.0)
		}
		var minValue = arr(0).toString.toDouble
		var maxValue = arr(0).toString.toDouble
		for(i <- 0 to arr.length-1){
			val item = arr(i).toString.toDouble
			if(item < minValue)
				minValue = item
			else if(item > maxValue)
				maxValue = item
		}
		(minValue, maxValue)
	}
}

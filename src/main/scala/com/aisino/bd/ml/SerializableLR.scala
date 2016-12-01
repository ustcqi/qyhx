package com.aisino.bd.ml

import breeze.numerics._
import com.aisino.bd.qyhx.mathUtil.MathUtils

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by QiChao on 11/23/16.
 */
object SerializableLR extends  Serializable{
	def getTrainDataDF(arr: Array[Any], sqlContext: SQLContext): DataFrame = {
		var list = List[(Double, DenseVector)]()
		//println(arr)
		val (minValue, maxValue) = MathUtils.getMinMaxOfArray(arr)
		val scale = pow(10, MathUtils.getNumDigits(minValue))
		println(minValue, maxValue, scale)
		for(i <- 0 to arr.length-1){
			val lirun = arr(i).asInstanceOf[Double]
			val tmpSeq = List((lirun.formatted("%.3f").toDouble/scale,  new DenseVector(Array(i+1))))
			list = list ++ tmpSeq
		}
		//list
		println(list)
		if(list == null){
			println("----list is null-------")
		}
		if(sqlContext == null){
			println("-----sqlContext is null")
		}

		val trainingDataDF = sqlContext.createDataFrame(list).toDF("label", "features")
		trainingDataDF
	}

	def train(df: DataFrame, sqlContext: SQLContext) : Double = {
		val lr = new LinearRegression()
            .setMaxIter(1000)
            //.setRegParam(0.2)
            .setLabelCol("label")
			.setFeaturesCol("features")
		val lrModel = lr.fit(df)
		val coefficients = lrModel.coefficients
		val w = coefficients(0)
		w.formatted("%.3f").toDouble
	}

}

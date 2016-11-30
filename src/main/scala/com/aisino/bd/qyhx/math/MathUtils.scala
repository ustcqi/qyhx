package com.aisino.bd.qyhx.math


/**
 * Created by Kerwin on 16-11-17.
 */

object MathUtils extends  Serializable{
	/**
	 * 求序列arr基于lag=k时的相关系数
	 * @param arr
	 * @param k
	 * @return  correlation coefficient (Double)
	 */
	def autocorrelation(arr: Array[Double], k: Int): Double ={
		val mean = arr.sum / arr.length
		//println(f"means = $mean%.6f")
		var variance = 0.0
		arr.foreach(x => {
			variance += (x - mean) * (x - mean)
		})
		//println(f"varance = $variance%.6f")
		var translatedVar = 0.0
		val n = arr.length
		for(i <- 0 until (n - k)){
			translatedVar += (arr(i) - mean) * (arr(i+k) - mean)
		}
		val correlation = (translatedVar / variance).formatted("%.6f").toDouble
		correlation
	}

	def getNumDigits(num: Double) : Int = {
		var n = num.toInt
		var digits = 1
		while((n / 10) != 0){
			n = n / 10
			digits = digits + 1
		}
		digits
	}

	def getMinMaxOfArray(arr: Array[Any]) : (Double, Double) = {
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

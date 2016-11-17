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
		//println(f"translated variance = $translatedVar%.6f")
		val correlation = (translatedVar / variance).formatted("%.6f").toDouble
		correlation
	}
}

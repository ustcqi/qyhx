package com.aisino.bd.qyhx.test

import com.aisino.bd.qyhx.{DataLoader, AppContext}
import com.aisino.bd.qyhx.ZQQY
/**
 * Created by kerwin on 16-11-18.
 */
object ZQQYTest {
	val usage =
		"""Usage: \n\t args(0): startTime \n\t args(1): endTime \n\t args(2): k \ n\t args(3) ratio

			 example: 201201 201312 12 0.2
		""".stripMargin.trim
	def main(args: Array[String]) {
		if (args.length < 4) {
			println(usage)
			sys.exit(1)
		}
		val startTime = args(0)
		val endTime = args(1)
		val k = args(2).toInt
		val ratio = args(3).toDouble

		val context = new AppContext()
		val dataLoader = new DataLoader(context)
		val nsrAyHzDF = dataLoader.getNsrAyHz()
		val zqqy = new ZQQY(context)

		val zqNsrDF = zqqy.zqNsr(nsrAyHzDF, startTime, endTime, k, ratio)
		zqNsrDF.rdd.foreach(println)
		println(zqNsrDF.count)

		context.sc.stop()
	}
}

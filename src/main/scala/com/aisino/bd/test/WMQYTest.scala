package com.aisino.bd.test

import com.aisino.bd.common.DataLoader
import com.aisino.bd.qyhx.{WMQY}
import com.aisino.bd.common.AppContext

/**
 * Created by kerwin on 16-11-18.
 */
object WMQYTest {
	val usage =
		"""Usage:
	            args(0): startTime
			    args(1): endTime

			 example:
	            201201 201312
		""".stripMargin.trim
	def main(args: Array[String]) {
		if(args.length < 2){
			println(usage)
			sys.exit(1)
		}
		val startTime = args(0)
		val endTime = args(1)
		val context = new AppContext()

		val dataLoader = new DataLoader(context)
		//val nsrDF = dataLoader.getWmNSRData()
		val nsrDF = dataLoader.getWmNsrData()
		val wm = new WMQY(context)
		val wmNsrDF = wm.wmNsr(nsrDF, startTime, endTime)
		wmNsrDF.rdd.foreach(println)
		context.sc.stop()
	}
}

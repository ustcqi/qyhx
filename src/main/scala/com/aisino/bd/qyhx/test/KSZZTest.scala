package com.aisino.bd.qyhx.test

import com.aisino.bd.qyhx.{DataLoader, AppContext}
import com.aisino.bd.qyhx.KSZZ
/**
 * Created by kerwin on 16-11-18.
 */
object KSZZTest {
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

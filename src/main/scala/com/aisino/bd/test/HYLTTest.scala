package com.aisino.bd.test

import com.aisino.bd.common.DataLoader
import com.aisino.bd.qyhx.{HYLT}
import com.aisino.bd.common.AppContext
/**
 * Created by kerwin on 16-11-18.
 */
object HYLTTest {
	val usage =
		"""Usage:
	            args(0): startTime
			    args(1): endTime
                args(2): n
			 example:
	            201201 201312 5
		""".stripMargin.trim
	def main(args: Array[String]) {
		if(args.length < 2){
			println(usage)
			sys.exit(1)
		}
		val startTime = args(0)
		val endTime = args(1)
		val n = args(2).toInt

		val context = new AppContext()

		val dataLoader = new DataLoader(context)

		val nsrAyHzDF = dataLoader.getNsrAyHz()
		val hyAyHzDF = dataLoader.getHyAyHz()
		val hylt = new HYLT(context)
		//val hyltDF = hylt.hyLtNsr(nsrAyHzDF, hyAyHzDF)
		val hyltDF = hylt.hyLtNsr(nsrAyHzDF, hyAyHzDF, startTime, endTime, n)
		hyltDF.write.mode("append").saveAsTable("dw_bak1.dw_dm_nsr_bq")
		//hyltDF.show()
		context.sc.stop()
	}
}

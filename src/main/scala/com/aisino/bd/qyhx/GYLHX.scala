package com.aisino.bd.qyhx

import com.aisino.bd.common.AppContext
import com.aisino.bd.common.DataLoader
import org.apache.spark.mllib.eigen
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.mllib.eigen.EigenVec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by kerwin on 16/10/11.
  */
class GYLHX(context: AppContext) extends Serializable{
    val sqlContext = context.sqlContext
    val sc = context.sc
    val spark = context.spark

    def gylhx(nsrHzDF: DataFrame): Unit ={
        val nsrByHyDF = nsrHzDF.groupBy("hy_dm").count
        //nsrByHyDF.printSchema() // hy_dm count
        val reverseNsrDF = nsrHzDF.select("hy_dm", "gf_nsrsbh", "xf_nsrsbh", "zje").toDF("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "zje")
        val unionNsrDF = nsrHzDF.union(reverseNsrDF)
                //.filter("xf_nsrsbh = '370303749864951'").show()

        //nsrByHyDF.collect().slice(0, 2).foreach(x => {
        nsrByHyDF.collect().foreach(x => {
            val hy_dm = x(0).toString
			//val hy_dm = "6820" //5523 hy_dm
			val unionNsrFilterDF = unionNsrDF.filter(s"hy_dm == '${hy_dm}'")
            val tmp1 = unionNsrFilterDF.select("gf_nsrsbh").rdd.map(x => x(0).toString).collect
            val tmp2 = unionNsrFilterDF.select("xf_nsrsbh").rdd.map(x => x(0).toString).collect
            val nsrIdxMap = (tmp1 ++ tmp2).toSet.toList.zipWithIndex.toMap.asInstanceOf[Map[String, Int]]
            val xfNsrDF = unionNsrFilterDF.groupBy("hy_dm", "xf_nsrsbh").pivot("gf_nsrsbh").sum("zje").cache()
            val columns = xfNsrDF.columns.toList
            val gfNsrNum = columns.length - 2
			if(gfNsrNum > 5) {
				//val matrixRdd = xfNsrDF.sample(false, 0.1).rdd.map(x => {
				val matrixRdd = xfNsrDF.rdd.map(x => {
					val nsrsbh = x(1).toString();
					val idxOfNsr = nsrIdxMap(nsrsbh)
					val idxArr = new ArrayBuffer[Int]()
					var valueArr = new ArrayBuffer[Double]()
					for (i <- 2 to gfNsrNum + 1) {
						if (x(i) != null) {
							idxArr += (i - 2)
							valueArr += x(i).toString.toDouble
						} else if (i == idxOfNsr) {
							idxArr += (i - 2)
							valueArr += 0.001
						}
					}
					//idxArr.foreach(println)
					val bsv = new BSV[Double](idxArr.toArray, valueArr.toArray, gfNsrNum)
					bsv
				})
				//k = 1
				val n = matrixRdd.count.toInt
				//println(n, gfNsrNum)
				val eigenvec = new EigenVec(matrixRdd, n, 1)
				val ev: BDV[Double] = eigenvec.se
				println(s"ev length = ${ev.length}, ev = ${ev}")
				//calculate the eigenvector bases on eigenvalue and maxtrixRDD
			}
        })

    }

}

object GYLHX {
    val usage =
        """Usage:
	            args(0): startTime
			    args(1): endTime
                args(2): table
			 example:
	            201201 201312 dw_bak1.dw_dm_nsr_bq1
        """.stripMargin.trim
    def main(args: Array[String]) {
        if(args.length < 3){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
        val tableName = args(2)

        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        val nsrHzDF = dataLoader.getXfToGfNsrDF(startTime, endTime)

        val gylhxObj = new GYLHX(context)
        gylhxObj.gylhx(nsrHzDF)
        context.sc.stop()
    }
}


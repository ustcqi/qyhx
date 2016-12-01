package com.aisino.bd.qyhx

import com.aisino.bd.Utils.{SchemaUtil, DateUtil}
import com.aisino.bd.common.AppContext
import com.aisino.bd.common.DataLoader
import org.apache.spark.mllib.eigen
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.mllib.eigen.EigenVec
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListMap
import scala.collection.mutable.Map
import scala.util.control.Breaks._

/**
  * Created by kerwin on 16/10/11.
  */
class GYLHX(context: AppContext) extends Serializable{
    val sqlContext = context.sqlContext
    val sc = context.sc
    val spark = context.spark

    def gylhx(nsrHzDF: DataFrame, startTime: String, endTime: String, ratio: Double): DataFrame ={
        val nsrByHyDF = nsrHzDF.groupBy("hy_dm").count
        //nsrByHyDF.printSchema() // hy_dm count
        val reverseNsrDF = nsrHzDF.select("hy_dm", "gf_nsrsbh", "xf_nsrsbh", "zje").toDF("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "zje")
        val unionNsrDF = nsrHzDF.union(reverseNsrDF)
                //.filter("xf_nsrsbh = '370303749864951'").show()
		var nsrList = List[(String, Double)]()
        nsrByHyDF.collect().slice(0, 100).foreach(x => {
        //nsrByHyDF.collect().foreach(x => {
            val hy_dm = x(0).toString
			//val hy_dm = "6820" //5523 hy_dm
			val unionNsrFilterDF = unionNsrDF.filter(s"hy_dm == '${hy_dm}'")
            val tmp1 = unionNsrFilterDF.select("gf_nsrsbh").rdd.map(x => x(0).toString).collect
            val tmp2 = unionNsrFilterDF.select("xf_nsrsbh").rdd.map(x => x(0).toString).collect
            val nsrIdxMap = (tmp1 ++ tmp2).toSet.toList.zipWithIndex.toMap.asInstanceOf[scala.collection.immutable.Map[String, Int]]
            val xfNsrDF = unionNsrFilterDF.groupBy("hy_dm", "xf_nsrsbh").pivot("gf_nsrsbh").sum("zje").cache()
			val nsrMatrix = xfNsrDF.rdd.map(x => x.toSeq.toArray.slice(2, x.length)).collect()//.asInstanceOf[Array[Array[Double]]] //Array[Array[Any]]
            val columns = xfNsrDF.columns.toList
			val xf_nsrsbh = columns(1).toString

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
					val bsv = new BSV[Double](idxArr.toArray, valueArr.toArray, gfNsrNum)
					bsv
				})
				//k = 1
				val n = matrixRdd.count.toInt
				//println(n, gfNsrNum)
				val eigenvec = new EigenVec(matrixRdd, n, 1)
				val (eigenvalueVec: BDV[Double], eigenvectorMat: BDM[Double]) = eigenvec.se
				println(s"eigenvalue length = ${eigenvalueVec.length}, eigenvalue = ${eigenvalueVec}")
				println(s"eigenvector vector length = ${eigenvectorMat.size}, eigenvecotr = ${eigenvectorMat}")
				/* calculate the node(nsr) eigenvector centrality of current hy */
				var centralityMap = Map[String, Double]()
				columns.slice(2, columns.length).foreach(x =>{
					val xf_nsrsbh = x.toString
					val (zje, edgeNumber) = eigenvectorCentrality(nsrMatrix, eigenvectorMat, nsrIdxMap, xf_nsrsbh, eigenvectorMat, columns)
					//val v = eigenvectorMat.apply(nsrIdxMap(xf_nsrsbh), 0)
					//println(eigenvectorMat.apply(0, 1)) //error
					//println(eigenvectorMat.apply(1, 0)) //correct
					centralityMap +=  (xf_nsrsbh -> zje)
				})
				val centralityArr = ListMap(centralityMap.toSeq.sortWith(_._2 > _._2):_*).toArray
				val hynsrList = centralityArr.slice(0, (0.1*centralityArr.length).toInt).toList
				nsrList = nsrList ++ hynsrList
				println(nsrList, "------", nsrList.length)
			}
        })
		val currentTime = DateUtil.getCurrentTime()
		val rdd = spark.sparkContext.parallelize(nsrList).map(x => Row(x._1.toString, 5.toString, x._2.toDouble, endTime, null, currentTime))
		val schema = SchemaUtil.nsrBqSchema
		val df = spark.createDataFrame(rdd, schema)
		df
    }

	def eigenvectorCentrality(nsrMatrix: Array[Array[Any]], eigenvectorMat: BDM[Double], nsrIdxMap: scala.collection.immutable.Map[String, Int], nsrsbh: String, eigenvector: BDM[Double], columns: List[String]) : (Double, Integer) = {
		var zje = 0.0
		var edgeNumber = 0
		columns.slice(2, columns.length).foreach(x => {
			val gf_nsrsbh = x.toString
			val (connected, je) = isConnected(nsrMatrix, eigenvectorMat: BDM[Double], nsrIdxMap, nsrsbh, gf_nsrsbh)
			if(connected) edgeNumber += 1
			zje += je
		})
		(zje, edgeNumber)
	}

	def isConnected(nsrMatrix: Array[Array[Any]], eigenvectorMat: BDM[Double], nsrIdxMap: scala.collection.immutable.Map[String, Int], nsrFrom: String, nsrTo: String) : (Boolean, Double) = {
		if(nsrFrom == nsrTo) (false, 0.0)

		val idxFrom = nsrIdxMap(nsrFrom)
		val idxTo = nsrIdxMap(nsrTo)
		val je = nsrMatrix(idxFrom)(idxTo)
		val v = eigenvectorMat.apply(nsrIdxMap(nsrTo), 0)
		//println("------ ", je, " ------- ", je.asInstanceOf[Double])
		//if(je.asInstanceOf[Double] == (null:Double))
		if(je.asInstanceOf[Double].toInt == 0) (false, 0.0)
		(true, v*je.asInstanceOf[Double])
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
        if(args.length < 4){
            println(usage)
            sys.exit(1)
        }
        val startTime = args(0)
        val endTime = args(1)
		val ratio = args(2).toDouble
        val tableName = args(3)


        val context = new AppContext()

        val dataLoader = new DataLoader(context)
        val nsrHzDF = dataLoader.getXfToGfNsrDF(startTime, endTime)

        val gylhxObj = new GYLHX(context)
        val df = gylhxObj.gylhx(nsrHzDF, startTime, endTime, ratio)
		df.show
        context.sc.stop()
    }
}


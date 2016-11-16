package com.aisino.bd.qyhx

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

    val path = "./data/file/output"
    import java.io.{File, FileWriter}
    @transient
    val pw = new FileWriter(new File(path)) // true / false (default)

    def gylhx(nsrHzDF: DataFrame): Unit ={
        //map(x => x(0).toString.substring(0, 2))
        val nsrByHyDF = nsrHzDF.groupBy("hy_dm").count
        val reverseNsrDF = nsrHzDF.select("hy_dm", "gf_nsrsbh", "xf_nsrsbh", "xxnje")
                .toDF("hy_dm", "xf_nsrsbh", "gf_nsrsbh", "xxnje")
        val unionNsrDF = nsrHzDF.union(reverseNsrDF)
                //.filter("xf_nsrsbh = '370303749864951'").show()

        /** Traverse all hy_dm*/
        nsrByHyDF.collect().slice(0, 2).foreach(x => {
            println(x.toString)
            val hy_dm = x(0).toString
            pw.write(hy_dm + "\n")
            pw.flush()
            /*
            val tmp1 = unionNsrDF.select("gf_nsrsbh").rdd.map(x => x(0).toString).collect
            val tmp2 = unionNsrDF.select("xf_nsrsbh").rdd.map(x => x(0).toString).collect
            val nsrIdxMap = (tmp1 ++ tmp2).toSet.toList.zipWithIndex.toMap
            */

            val xfNsrDF = unionNsrDF.filter(s"hy_dm = $hy_dm")
                    .groupBy("hy_dm", "xf_nsrsbh")
                    .pivot("gf_nsrsbh")
                    .sum("xxnje")
                    .cache()

            //xfNsrDF.filter("xf_nsrsbh = ")
            //println(xfNsrDF.columns, xfNsrDF.schema)
            val columns = xfNsrDF.columns.toList
            val gfNsrNum = columns.length - 2

            println(xfNsrDF.sample(false, 0.5).count)
            println(xfNsrDF.count)
            val matrixRdd = xfNsrDF.sample(false, 0.1).rdd.map(x => {
                val idxArr = new ArrayBuffer[Int]()
                var valueArr = new ArrayBuffer[Double]()
                for (i <- 2 to gfNsrNum) {
                    if (x(i) != null) {
                        idxArr += i
                        valueArr += x(i).toString.toDouble
                        //idxArr += nsrIdxMap(columns(i))
                    }
                }
                idxArr.foreach(println)
                val bsv = new BSV[Double](idxArr.toArray, valueArr.toArray, gfNsrNum)
                println(bsv)
                bsv
            })
            //k = 1
            println(matrixRdd.count(), gfNsrNum)
            val eigenvec = new EigenVec(matrixRdd, gfNsrNum, 1)
            val ev: BDV[Double] = eigenvec.se
            print(s"ev length: ${ev.length}, ev = ")
            ev.foreach(x => pw.write(x.toString + "\t"))
            pw.write("\n")
            pw.flush()
        })
    }
}

object GYLHX {
    def main(args: Array[String]): Unit ={
        val context = new AppContext()
        /*
        val dataLoader = new DataLoader(context)

        val xxfpDF = dataLoader.getXXFPData()
        val jxfpDF = dataLoader.getJXFPData()
        val nsrDF = dataLoader.getNSRData()

        val ds = new DataSummary(context)
        val nsrAyHzDF = ds.nsrToNsrAyHz(xxfpDF, jxfpDF, nsrDF)
        val anDF = ds.nsrToNsrHz(nsrAyHzDF)

        //anDF.write.saveAsTable("nsrHzTable")
        //anDF.createOrReplaceTempView("nsrHzTable")
        //anDF.write.save("./data/table/nsrHzTable.parquet")
        */
        val nsrHzDF = context.sqlContext.read.load("./data/table/nsrHzTable.parquet")
        /*
        val func_hy_dm_substr2 :(String => String) = (arg : String) => arg.substring(0, 2)
        val udf_hy_dm_substr2 = udf(func_hy_dm_substr2)
        val nsrHzDF1 = nsrHzDF.withColumn("hy_dm", udf_hy_dm_substr2(col("hy_dm")))
        */
        val gylhxObj = new GYLHX(context)
        gylhxObj.gylhx(nsrHzDF)
        context.sc.stop()
    }
}


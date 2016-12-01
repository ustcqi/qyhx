package org.apache.spark.mllib.eigen

import org.apache.spark.rdd.RDD
import breeze.linalg.{axpy => brzAxpy, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.mllib.linalg.EigenValueDecomposition

/**
  * Created by kerwin on 16/10/20.
  */

class EigenVec(val data: RDD[BSV[Double]], val n: Int, val k: Int) {
    def multiplyGramianMatrixBy(v: BDV[Double]): BDV[Double] = {
        val vbr = data.context.broadcast(v)
        //println("data is:")
        //data.collect().foreach(println)
        data.treeAggregate(BDV.zeros[Double](n))(
            seqOp = (U, rBrz) => {
                //println("rBrz and vbr.value")
               // println(rBrz, vbr.value)
                val a = rBrz.dot(vbr.value)
                brzAxpy(a, rBrz, U)
                U
            }, combOp = (U1, U2) => U1 += U2)
    }

    def se : (BDV[Double], BDM[Double]) = {
        val (r1, r2) = EigenValueDecomposition.symmetricEigs(multiplyGramianMatrixBy, n, k, 1e-3, 200)
        (r1, r2)
    }
}
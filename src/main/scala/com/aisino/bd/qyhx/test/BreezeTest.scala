package com.aisino.bd.qyhx.test

import breeze.linalg.{SparseVector, _}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import scala.collection.mutable.ArrayBuffer
/**
  * Created by kerwin on 16/10/28.
  */
class BreezeTest {
    def func(): Unit ={
        /*
        val denseVec = DenseVector.zeros[Double](10)
        denseVec(2) = 5.0
        denseVec(3 to 5) := 6.0
        println(denseVec)

        val sparseVec = SparseVector.zeros[Double](10)
        sparseVec(4) = 2
        sparseVec(5) = 9.0
        println(sparseVec)

        val denseMat = DenseMatrix.zeros[Int](5, 5)
        println(denseMat.rows, denseMat.cols)
        denseMat(4, ::) := DenseVector(1, 2, 3, 4, 5).t
        println(denseMat)
        denseMat(::, 1) := DenseVector(5, 4, 3, 2, 1)
        println(denseMat)
        denseMat(0 to 1, 0 to 1) := DenseMatrix((3, 1), (-1, -2))
        println(denseMat)
        */
        val idxArr = new ArrayBuffer[Int]()
        var valueArr = new ArrayBuffer[Double]()
        idxArr += (3, 4, 5, 9, 21, 100, 104, 200, 201, 299, 2999)
        valueArr += (5, 5, 6, 6, 7, 3, 4, 5, 6, 299, 2999)
        val gfNsrNum = 3000
        val bsv = new BSV[Double](idxArr.toArray, valueArr.toArray, gfNsrNum)
                //.asInstanceOf[BSV[Double]]
        bsv.foreach(println)
    }
}

object BreezeTest{
    def main(args: Array[String]): Unit ={
        val breeze = new BreezeTest()
        breeze.func()
    }
}

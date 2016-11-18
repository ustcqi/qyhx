package com.aisino.bd.qyhx.common

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame

/**
  * Created by kerwin on 16/10/8.
  */
object DateUtil {
    def getCurrentTime(): String = {
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val date = new Date()
        formatter.format(date)
        //println(formatter.format(date))
    }
}

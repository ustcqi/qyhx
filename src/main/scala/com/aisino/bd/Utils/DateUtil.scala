package com.aisino.bd.Utils

import java.text.SimpleDateFormat
import java.util.Date

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

    def parseDate(s: String): String ={
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        val ym = new SimpleDateFormat("yyyyMM")
        ym.format(sdf.parse(s))
    }
}

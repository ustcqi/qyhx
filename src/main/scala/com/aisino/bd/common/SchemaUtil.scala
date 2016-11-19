package com.aisino.bd.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Created by kerwin on 16-11-18.
 */
object SchemaUtil {
	val nsrBqSchema = StructType(
		StructField("nsr_key", StringType, false)
			:: StructField("bq_key", StringType, false)
			:: StructField("bq_value", StringType, false)
			:: StructField("bq_start_time", StringType, false)
			:: StructField("bq_end_time", StringType, true)
			:: StructField("tag_time", StringType, false)
			:: Nil)
}

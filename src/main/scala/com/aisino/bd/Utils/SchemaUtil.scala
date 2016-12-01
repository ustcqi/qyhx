package com.aisino.bd.Utils

import org.apache.spark.sql.types._

/**
 * Created by kerwin on 16-11-18.
 */
object SchemaUtil {
	val nsrBqSchema = StructType(
		StructField("nsr_key", StringType, false)
			:: StructField("bq_key", StringType, false)
			:: StructField("bq_value", DoubleType, false)
			:: StructField("bq_start_time", StringType, false)
			:: StructField("bq_end_time", StringType, true)
			:: StructField("tag_time", StringType, false)
			:: Nil)

	val gylhxNsrSchema = StructType(
		StructField("dy_dm", StringType, false)
			:: StructField("nsrsbh", StringType, false)
			:: StructField("centrality", DoubleType, false)
			//:: StructField("edge_number", IntegerType, false)
			:: Nil)
}

package com.objectfrontier.health.analytics.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class DataFrameGenerator(sqlCtx:SQLContext,keyspaceName :String, tableName: String) {

	val df = sqlCtx.read
			.format("org.apache.spark.sql.cassandra")
			.options(Map( "table" -> tableName, "keyspace" -> keyspaceName))
			.load()

			def getDataFrame() = {
		df
	}


	def getUCIImputedDataframe() = {
		// Remove Time_Stamp Column here as it is non-important attribute
		val df_minus_timestamp = df.drop("time_stamp")

				//Fill the empty column with "NA"
				val df1 = df_minus_timestamp.na.replace("heart_rate", ImmutableMap.of("NA", "0"));

		//Filter Heart Rate
		val df_imputed = df1.filter("heart_rate != 0")

				df_imputed
	}

	def getUCIPreparedDataframe(df_imputed : org.apache.spark.sql.DataFrame ) = {

		def encodeLabel=udf((activity_id: Double) => {
			activity_id match {
			case 1 => 0.0 
			case 2 => 1.0
			case 3 => 2.0
			case 4 => 3.0
			case 5 => 4.0
			case 6 => 5.0
			case 7 => 6.0
			case 9 => 7.0
			case 10 => 8.0
			case 11 => 9.0
			case 12 => 10.0
			case 13 => 11.0
			case 16 => 12.0
			case 17 => 13.0
			case 18 => 14.0
			case 19 => 15.0
			case 20 => 16.0
			case 24 => 17.0
			//case 0 => 18.0
			case _ => 18.0
			}})

			def toVec4 =udf((heart_rate: Double,
					IMU_hand_temperature: Double) => {
						Vectors.dense(heart_rate,
								IMU_hand_temperature)})


								val df_prepared = df_imputed.withColumn(
										"features",
										toVec4(
												df_imputed("heart_rate"),
												df_imputed("IMU_hand_temperature"))
										).withColumn("label", encodeLabel(df_imputed("activity_id")))
										.select("features", "label")

										df_prepared

	}
}
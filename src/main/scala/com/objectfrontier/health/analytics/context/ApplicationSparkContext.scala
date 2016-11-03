package com.objectfrontier.health.analytics.context

//// Basic Spark Library
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import com.typesafe.config.ConfigFactory


object ApplicationSparkContext {

	val config = ConfigFactory.load()
			val cassandraConnectionHost = config.getString("cassandra.connection.host")
			val mesosMaster = config.getString("mesos.master")

			val conf = new SparkConf(true)
	.set("spark.cassandra.connection.host", cassandraConnectionHost)
	.setAppName(this.getClass.getSimpleName)
	//.setMaster("spark://10.10.40.138:7077")
	//.setMaster(mesosMaster)

	val sparkContext = new SparkContext(conf)  

	def getApplicationSparkContext() = {
		sparkContext
	}
}
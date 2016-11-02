package com.objectfrontier.health.analytics.main

//// Basic Spark Library
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//// Spark-Cassandra Connector Library
import com.datastax.spark.connector._

//// Spark-SQL Library
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

//// MLLib (Spark Machine Learning Libraries)
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import com.typesafe.config.ConfigFactory

object Main {
  def main(args: Array[String]) {

    val config = ConfigFactory.load()
    val cassandraConnectionHost = config.getString("cassandra.connection.host")

    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", cassandraConnectionHost)
        .setAppName(this.getClass.getSimpleName)
        //  .setMaster("spark://10.10.40.138:7077")
        // .setMaster("mesos://zk://10.10.40.138:2181/mesos")

        val sc =  new SparkContext(conf)
    
     // Spark-SQL Context
        val sqlCtx = new SQLContext(sc)

        val emr_labscorepopulated_rdd = sc.cassandraTable("emrbots_data", "emr_labscorepopulated")
        
        //println("/////////////////////////////////////// " + emr_labscorepopulated_rdd.count())
        
        // Total number of records in this table = 107535277
        emr_labscorepopulated_rdd.take(20).foreach(println)

      

        sc.stop()
  }

}


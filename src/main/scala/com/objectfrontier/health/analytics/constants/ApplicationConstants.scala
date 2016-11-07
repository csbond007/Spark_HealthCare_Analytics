package com.objectfrontier.health.analytics.constants


import com.typesafe.config.ConfigFactory

object ApplicationConstants {
  
  val Config = ConfigFactory.load()
  
  val CASSANDRA_CONNECTION_HOST = Config.getString("cassandra.connection.host")
  
  val MESO_MASTER = Config.getString("mesos.master")
  
  val KEYSPACE_UCI = Config.getString("keyspace.uci")
  
  val UCI_PROTOCOLSUBJECT109 = Config.getString("uci.protocolsubject.109")
  
  val NUM_CLASSES = Config.getInt("numClasses")
  
  val CATEGORICAL_FEATURES_INFO = Map[Int, Int]()
  
  val IMPURITY = Config.getString("impurity")
  
  val MAX_DEPTH = Config.getInt("maxDepth")
  
  val MAX_BINS = Config.getInt("maxBins")
  
  // Extra Parameters for Random Forest
  val NUM_TREES = Config.getInt("numTrees")
  
  val FEATURE_SUBSET_STRATEGY = Config.getString("featureSubsetStrategy") 
}
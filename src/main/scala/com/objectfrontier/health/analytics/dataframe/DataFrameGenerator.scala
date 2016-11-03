package com.objectfrontier.health.analytics.dataframe

import org.apache.spark.sql.SQLContext

class DataFrameGenerator(sqlCtx:SQLContext,keyspaceName :String, tableName: String) {
  
  val df = sqlCtx.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> tableName, "keyspace" -> keyspaceName))
    .load()
    
    def getDataFrame() = {
     df
    }
}
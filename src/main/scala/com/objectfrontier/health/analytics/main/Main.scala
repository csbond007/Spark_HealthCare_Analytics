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

import com.objectfrontier.health.analytics.context.ApplicationSparkContext
import com.objectfrontier.health.analytics.dataframe.DataFrameGenerator
import com.objectfrontier.health.analytics.constants.ApplicationConstants
import com.objectfrontier.health.analytics.algorithms.RandomForestAlgorithm

object Main {
	def main(args: Array[String]) {

		val sc =  ApplicationSparkContext.getApplicationSparkContext()

				var dataFrameObj : DataFrameGenerator = new DataFrameGenerator(new SQLContext(sc),
						ApplicationConstants.KEYSPACE_UCI,
						ApplicationConstants.UCI_PROTOCOLSUBJECT109);

		val df_imputed = dataFrameObj.getUCIImputedDataframe()

				val df_prepared = dataFrameObj.getUCIPreparedDataframe(df_imputed)


				println("&&&&&&&&&&&&&&&&&&&&&&&&&  =  " + df_prepared.count())
				//df_imputed.



				df_prepared.show(10)   

				//////////////////////////////////////////////////////////////////////////////

      val (trainingData, testData) = get_Training_Testing_Data(df_prepared)

				

								//  trainingData.cache()
								//  testData.cache()

								// Train a DecisionTree model.
								//  Empty categoricalFeaturesInfo indicates all features are continuous.

								val randomForestAlgorithmObj = new RandomForestAlgorithm()



		val model = randomForestAlgorithmObj.getDataModel(trainingData, ApplicationConstants.NUM_CLASSES, 
				ApplicationConstants.CATEGORICAL_FEATURES_INFO,
				ApplicationConstants.NUM_TREES, 
				ApplicationConstants.FEATURE_SUBSET_STRATEGY, 
				ApplicationConstants.IMPURITY, 
				ApplicationConstants.MAX_DEPTH, 
				ApplicationConstants.MAX_BINS)

				// val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
						// impurity, maxDepth, maxBins)

      calculatePredictionAccuracy(testData,model)

				sc.stop()
	} // end-main method

	def calculatePredictionAccuracy(testData : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
			                            model    : org.apache.spark.mllib.tree.model.RandomForestModel) = {

		val labelAndPreds = testData.map { point =>
		val prediction = model.predict(point.features)
		(point.label, prediction)
		}

		val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    
    val prediction_Accuracy = (100-testErr*100)
				
    println("Prediction Accuracy ////////////////////////////////////////////// = " + prediction_Accuracy + "%")
	}
  
  def get_Training_Testing_Data(df_prepared : org.apache.spark.sql.DataFrame): (org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint],
                                                                                  org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]) =
  {
    val splits = df_prepared.randomSplit(Array(0.7, 0.3))

        val (training_split, test_split) = (splits(0), splits(1))

        val trainingData = training_split.rdd.map(row => LabeledPoint(
            row.getAs[Double]("label"),
            row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
            ))

       //     println("+++++++++++++++++++++++++++++++++++++++++++++++++++++" + trainingData.count()) 

            val testData = test_split.rdd.map(row => LabeledPoint(
                row.getAs[Double]("label"),
                row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
                ))
                
       (trainingData, testData)         
  } // end get_Training_Testing_Data

}

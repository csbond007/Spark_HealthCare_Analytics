package com.objectfrontier.health.analytics.algorithms


import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import com.objectfrontier.health.analytics.constants.ApplicationConstants
import org.apache.spark.rdd.RDD

class RandomForestAlgorithm {


	def getDataModel(trainingData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], numClasses: Int,categoricalFeaturesInfo: Map[Int,Int],
      numTrees: Int,featureSubsetStrategy: String, impurity: String, maxDepth: Int, maxBins: Int) = {

		val model = RandomForest.trainClassifier(trainingData, numClasses, 
				categoricalFeaturesInfo,numTrees, 
				featureSubsetStrategy, impurity, maxDepth, maxBins)

		model
	}
  
  def calculateRandomForestPredictionAccuracy(testData : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
      model    : org.apache.spark.mllib.tree.model.RandomForestModel) = {

    val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()

        val prediction_Accuracy = (100-testErr*100)

        println("RandomForestAlgorithm Prediction Accuracy ////////////////////////////////////////////// = " + prediction_Accuracy + "%")
  }
} 
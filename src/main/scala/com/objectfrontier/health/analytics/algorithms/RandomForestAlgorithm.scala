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
} 
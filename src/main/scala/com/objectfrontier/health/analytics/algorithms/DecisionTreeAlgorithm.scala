package com.objectfrontier.health.analytics.algorithms


import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LabeledPoint
import com.objectfrontier.health.analytics.constants.ApplicationConstants
import org.apache.spark.rdd.RDD

class DecisionTreeAlgorithm {


	def getDataModel(trainingData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
                   numClasses: Int,categoricalFeaturesInfo: Map[Int,Int],
                   impurity: String, maxDepth: Int, maxBins: Int) = {
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

		model
	}
  
  def calculateDecisionTreePredictionAccuracy(testData : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
      model    : org.apache.spark.mllib.tree.model.DecisionTreeModel) = {

    val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()

    val prediction_Accuracy = (100-testErr*100)

    println("DecisionTreeAlgorithm Prediction Accuracy ////////////////////////////////////////////// = " + prediction_Accuracy + "%")
  }
} 
package com.objectfrontier.health.analytics.main


//// Basic Spark Library
import org.apache.spark.sql.SQLContext


import com.objectfrontier.health.analytics.context.ApplicationSparkContext
import com.objectfrontier.health.analytics.dataframe.UCIDataFrameGenerator
import com.objectfrontier.health.analytics.constants.ApplicationConstants
import com.objectfrontier.health.analytics.algorithms.RandomForestAlgorithm
import com.objectfrontier.health.analytics.algorithms.DecisionTreeAlgorithm

object Main {
	def main(args: Array[String]) {

		    val sc =  ApplicationSparkContext.getApplicationSparkContext()

				val dataFrameObj = new UCIDataFrameGenerator(new SQLContext(sc),
						ApplicationConstants.KEYSPACE_UCI,
						ApplicationConstants.UCI_PROTOCOLSUBJECT109);

		    val df_imputed = dataFrameObj.getUCIImputedDataframe()

				val df_prepared = dataFrameObj.getUCIPreparedDataframe(df_imputed)

				val (trainingData, testData) = dataFrameObj.getTrainingAndTestingData(df_prepared)

				if(ApplicationConstants.ALGORITM_CHOICE.equalsIgnoreCase("randomforest")) {

					val randomForestAlgorithmObj = new RandomForestAlgorithm()
          
					val randomForestModel = randomForestAlgorithmObj.getDataModel(trainingData, ApplicationConstants.NUM_CLASSES, 
							ApplicationConstants.CATEGORICAL_FEATURES_INFO,
							ApplicationConstants.NUM_TREES, 
							ApplicationConstants.FEATURE_SUBSET_STRATEGY, 
							ApplicationConstants.IMPURITY, 
							ApplicationConstants.MAX_DEPTH, 
							ApplicationConstants.MAX_BINS)

					randomForestAlgorithmObj.calculateRandomForestPredictionAccuracy(testData,randomForestModel)                       

				} else if(ApplicationConstants.ALGORITM_CHOICE.equalsIgnoreCase("decisiontree")) {


					val decisionTreeAlgorithmObj = new DecisionTreeAlgorithm()

					val decisionTreeModel = decisionTreeAlgorithmObj.getDataModel(trainingData, ApplicationConstants.NUM_CLASSES,
							ApplicationConstants.CATEGORICAL_FEATURES_INFO,
							ApplicationConstants.IMPURITY,
							ApplicationConstants.MAX_DEPTH,
							ApplicationConstants.MAX_BINS)
              
          decisionTreeAlgorithmObj.calculateDecisionTreePredictionAccuracy(testData,decisionTreeModel)
				}  

		    sc.stop()

	} 

}

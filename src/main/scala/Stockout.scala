import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Stockout {
  {
    Logger.getLogger("org").setLevel(Level.WARN)
  }

  private val sc: SparkContext = new SparkContext("local[2]", "Stockout")

  def main(args: Array[String]) {
    val rawData: RDD[String] = sc.textFile("aggregated.csv")
    val records: RDD[Array[String]] = rawData.map(_.split('\t'))
    //    facility product month tx_in quantity_in tx_out quantity_out stockout
    //    10	552	2016-03	1	0	1	30	True

    val allFacilities: Map[String, Long] = records.map(_ (0)).distinct().zipWithIndex().collect().toMap
    val allProducts: Map[String, Long] = records.map(_ (1)).distinct().zipWithIndex().collect().toMap

    val dataSet: RDD[LabeledPoint] = records.map { record =>
      val facilityFeatures: Array[Double] = Array.ofDim[Double](allFacilities.size)
      facilityFeatures(allFacilities(record(0)).toInt) = 1.0

      val productFeatures: Array[Double] = Array.ofDim[Double](allProducts.size)
      productFeatures(allProducts(record(1)).toInt) = 1.0

      val monthFeature: Double = Math.sin(record(2).substring(5).toInt * 2 * 3.14159 / 12)
      //      val monthFeature: Double = record(2).substring(5).toDouble

      val numericFeatures: Array[Double] =
        Array(record(3).toDouble, record(4).toDouble, record(5).toDouble, record(6).toDouble)
      val label: Double = if (record(7) == "True") 1.0 else 0.0
      val allFeatures: Array[Double] = facilityFeatures ++ productFeatures ++ Array(monthFeature) ++ numericFeatures
      //      val allFeatures: Array[Double] = facilityFeatures ++ productFeatures ++ numericFeatures
      LabeledPoint(label, Vectors.dense(allFeatures))
    }

    val standardScaler: StandardScaler = new StandardScaler(withMean = true, withStd = true)
    val standardScalerModel: StandardScalerModel = standardScaler.fit(dataSet.map(_.features))
    val standardizedDataSet: RDD[LabeledPoint] = dataSet.map(dataPoint =>
      LabeledPoint(dataPoint.label, standardScalerModel.transform(dataPoint.features)))

    standardizedDataSet.cache()
    val positiveDataSet: RDD[LabeledPoint] = standardizedDataSet.filter(_.label == 1.0)

    val svmWithSGD: SVMWithSGD = new SVMWithSGD()
    svmWithSGD.optimizer.setUpdater(new SquaredL2Updater).setNumIterations(20).setStepSize(0.1)
    val svmModel: SVMModel = svmWithSGD.run(standardizedDataSet)
    printMetrics(svmModel, standardizedDataSet)
    printMetrics(svmModel, positiveDataSet)
  }

  private def printMetrics(model: ClassificationModel, dataSet: RDD[LabeledPoint]): Unit = {
    val predictionsVsActuals: RDD[(Double, Double)] = dataSet.map(
      dataPoint => {
        (model.predict(dataPoint.features), dataPoint.label)
      })
    val metrics: BinaryClassificationMetrics = new BinaryClassificationMetrics(predictionsVsActuals)

    println("%s, Accuracy: %2.4f%%, Area under PR: %2.4f%%, Area under ROC: %2.4f%%"
      .format(model.getClass.getSimpleName, accuracy(model, dataSet) * 100, metrics.areaUnderPR() * 100, metrics.areaUnderROC() * 100))
  }

  private def accuracy(model: ClassificationModel, dataSet: RDD[LabeledPoint]): Double = {
    val predictionResults: RDD[Double] = dataSet.map(dataPoint => {
      if (model.predict(dataPoint.features) == dataPoint.label) 1.0 else 0.0
    })
    predictionResults.sum / dataSet.count()
  }
}
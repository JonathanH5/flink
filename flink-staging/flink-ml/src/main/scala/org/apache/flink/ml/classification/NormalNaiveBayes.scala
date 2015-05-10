package org.apache.flink.ml.classification

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.common._
import org.apache.flink.api.scala._

/**
 * Created by jonathanhasenburg on 05.05.15.
 */
class NormalNaiveBayes extends Learner[(String, String), NormalNaiveBayesModel] {

  override def fit(input: DataSet[(String, String)], fitParameters: ParameterMap): NormalNaiveBayesModel = {

    val documentsPerClass: DataSet[(String, Int)] = input.map { className => (className._1, 1) }
      .groupBy(0)
      .sum(1)

    documentsPerClass.writeAsText("/Users/jonathanhasenburg/Desktop/documentsPerClass.txt")
    print("wrote the documentsPerClass dataset")

    return new NormalNaiveBayesModel()
  }


}

class NormalNaiveBayesModel extends Transformer[String, (String, String)] {
  override def transform(input: DataSet[String], transformParameters: ParameterMap): DataSet[(String, String)] = {

    return null
  }
}
package org.apache.flink.ml.classification

import org.apache.flink.api.common.functions.{ReduceFunction, FlatMapFunction, MapFunction}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * Created by jonathanhasenburg on 05.05.15.
 */
class NormalNaiveBayes extends Learner[(String, String), NormalNaiveBayesModelOneString] with Serializable {

 override def fit(input: DataSet[(String, String)], fitParameters: ParameterMap): NormalNaiveBayesModelOneString = {


    //******************************************************************************************************************
    //gather all information from the input data

    // Classname -> Count of documents
    val documentsPerClass: DataSet[(String, Int)] = input.map { line => (line._1, 1) }
      .groupBy(0)
      .sum(1)

    // Classname -> Word -> Count of that word
    val singleWordsInClass: DataSet[(String, String, Int)] = input.flatMap(new SingleWordSplitter())
      .groupBy(0, 1)
      .sum(2)

    // Classname -> Count of all words in class
    val allWordsInClass : DataSet[(String, Int)] = singleWordsInClass.groupBy(0).reduce {
      (line1, line2) => (line1._1, line1._2, line1._3 + line2._3)}.map(line => (line._1, line._3))

    // Classname -> Word -> Count of that word -> Count of all words in class
    val wordsInClass = singleWordsInClass.join(allWordsInClass).where(0).equalTo(0) {
      (single, all) => (single._1, single._2, single._3, all._2)
    }

    // A one for each document {1, 1, 1, ...}
    val documentsCountHelperSet = documentsPerClass.flatMap(new DocumentCounterByCreatingEntries())
    val documentsCount: Double = documentsCountHelperSet.count

    // All words, but distinct
    val vocabulary = singleWordsInClass.map(tuple => (tuple._2, 1)).distinct(0)
    val vocabularyCount: Double = vocabulary.count

    //******************************************************************************************************************
    //calculate P(w) and P(w|c)

    // Classname -> P(w)
    val pw: DataSet[(String, Double)] = documentsPerClass.map(line => (line._1, line._2 / documentsCount))

    // Classname -> Word -> P(w|c)
    val pwc : DataSet[(String, String, Double)] = wordsInClass.map(line => (line._1, line._2, ((line._3 + 1) / (line._4 + vocabularyCount))))

    // Classname -> Word -> P(w|c) -> P(w)
    val probabilityDataSet = pwc.join(pw).where(0).equalTo(0) {
      (pwc, pw) => (pwc._1, pwc._2, pwc._3, pw._2)
    }

    probabilityDataSet.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/probabilityDataSet.txt", WriteMode.OVERWRITE)


    //return new NormalNaiveBayesModel(probabilityDataSet)
    return new NormalNaiveBayesModelOneString(probabilityDataSet)

  }

  /*
  * ******************************************************************************************************************
  * *******************************************Function Classes*******************************************************
  * ******************************************************************************************************************
   */

  /**
   * Transforms a (String, String) tuple into a (String, String, Int)) tuple.
   * The second string from the input gets split into it words, for each word a tuple is collected with the Int (count) 1
   */
  class SingleWordSplitter() extends FlatMapFunction[(String, String), (String, String, Int)] {
    override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {
      for (token: String <- value._2.split(" ")) {
        out.collect((value._1, token, 1))
      }
    }
  }

  /**
   * Creates a Dataset with a 1 for each document by submitting a 1 value._2 amount of times for each value._1
   */
  class DocumentCounterByCreatingEntries() extends FlatMapFunction[(String, Int), Int] {
    override def flatMap(value: (String, Int), out: Collector[Int]): Unit = {
      for (i <- 1 to value._2) {
        out.collect(1)
      }
    }
  }


}


class NormalNaiveBayesModel(probabilityDataSet: DataSet[(String, String, Double, Double)]) extends Transformer[String, (String, String)] {

  override def transform(input: DataSet[String], transformParameters: ParameterMap): DataSet[(String, String)] = {

    /*
       First step: Take a String from the input DataSet
       Second step: Split it into its words
       Third step: Calculate Probability for each class -> P(c) * Sum of all P(w|c)
       Fourth step: Select class with highest Probability
       Fifth step: Repeat until finished
     */

    //Split
    //val inputSet = input.flatMap { _.split(" ") }.map(line => (line, ""))

    //val inputSet = input.map(line => (line, 1))

    //val reducedProbabilityDataSet = inputSet.join(probabilityDataSet).where(0).equalTo(1)
    //reducedProbabilityDataSet.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/reducedProbabilityDataSet.txt", WriteMode.OVERWRITE)



    return null
  }
}

class NormalNaiveBayesModelOneString(probabilityDataSet: DataSet[(String, String, Double, Double)]) extends Transformer[String, (String, String)] {
  override def transform(input: DataSet[String], transformParameters: ParameterMap): DataSet[(String, String)] = {

    val words: DataSet[(String, Int)] = input.flatMap { _.split(" ") }.map{line => (line, 1)}.groupBy(0).sum(1)
    val joinedWords = words.join(probabilityDataSet).where(0).equalTo(1) //join probabilityDataSet and words

    //TODO problem: we loose words not in class

    joinedWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/joinedWords.txt", WriteMode.OVERWRITE)

    return null
  }
}

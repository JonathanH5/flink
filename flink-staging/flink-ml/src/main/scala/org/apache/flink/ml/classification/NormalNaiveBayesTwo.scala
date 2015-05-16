package org.apache.flink.ml.classification

import org.apache.flink.api.common.functions.{RichMapFunction, FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.util.Collector
import scala.collection.mutable.Map


/**
 * Created by jonathanhasenburg on 05.05.15.
 */
class NormalNaiveBayesTwo extends Learner[(String, String), NormalNaiveBayesModelTwoOneString] with Serializable {

 override def fit(input: DataSet[(String, String)], fitParameters: ParameterMap): NormalNaiveBayesModelTwoOneString = {

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

   allWordsInClass.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/allWordsInClass.txt", WriteMode.OVERWRITE)


   // Classname -> Word -> Count of that word -> Count of all words in class
   val wordsInClass = singleWordsInClass.join(allWordsInClass).where(0).equalTo(0) {
     (single, all) => (single._1, single._2, single._3, all._2)
   }


   // Count of all documents
   val documentsCount: Double= input.count()

   // All words, but distinct
   val vocabulary = singleWordsInClass.map(tuple => (tuple._2, 1)).distinct(0)
   val vocabularyCount: Double = vocabulary.count

   //******************************************************************************************************************
   //calculate P(w) and P(w|c)

   // Classname -> P(w) -> p(w|c) word not in class
   val pw: DataSet[(String, Double)] = documentsPerClass.map(line => (line._1, line._2 / documentsCount))

   // Classname -> Pwc word not in class
   val pwcNotInClass : DataSet[(String, Double)] = allWordsInClass.map(line => (line._1, 1 / (line._2 + vocabularyCount)))
   pwcNotInClass.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/pwcNotInClass.txt", WriteMode.OVERWRITE)

   // Classname -> p(c) -> p(w|c) not in class
   val classInfo = pw.join(pwcNotInClass).where(0).equalTo(0) {
     (line1, line2) => (line1._1, line1._2, line2._2)
   }

   // Classname -> Word -> P(w|c)
   val pwc : DataSet[(String, String, Double)] = wordsInClass.map(line => (line._1, line._2, ((line._3 + 1) / (line._4 + vocabularyCount))))

   // Classname -> Word -> log(P(w|c)) -> log(P(c)) -> log(p(w|c)) not in class
   val probabilityDataSet = pwc.join(classInfo).where(0).equalTo(0) {
     (pwc, classInfo) => (pwc._1, pwc._2, Math.log(pwc._3), Math.log(classInfo._2), Math.log(classInfo._3))
   }


   //TODO HOW do I put the information in here?

   //Create a DataSet with classPojos and fill it with the classInfo information?
   //Then a custom Join function to fill the wordToPWC map?
  //val outputs : DataSet[classPoJo] = probabilityDataSet.map(line => )
   probabilityDataSet.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/probabilityDataSet.txt", WriteMode.OVERWRITE)

   return new NormalNaiveBayesModelTwoOneString(probabilityDataSet)

  }

  /*
  * ******************************************************************************************************************
  * *************************************************POJO*************************************************************
  * ******************************************************************************************************************
 */

  class classPoJo {
    val pc: Double = 0.0
    val pwcNotInClass: Double = 0.0
    var wordToPwcMap = Map[String, Double]()
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


class NormalNaiveBayesModelTwo(probabilityDataSet: DataSet[(String, String, Double, Double, Double)]) extends Transformer[String, (String, String)] {

  override def transform(input: DataSet[String], transformParameters: ParameterMap): DataSet[(String, String)] = {



    return null
  }
}

class NormalNaiveBayesModelTwoOneString(probabilityDataSet: DataSet[(String, String, Double, Double, Double)]) extends Transformer[String, (String, String)] {
  override def transform(input: DataSet[String], transformParameters: ParameterMap): DataSet[(String, String)] = {

    val words : DataSet[(String)] = input.flatMap { _.split(" ") }

    val wordsInInput = words.count()

    //split input text in words
    val wordsAndCount: DataSet[(String, Int)] = words.map{line => (line, 1)}.groupBy(0).sum(1)

    //join probabilityDataSet and words to classname -> Word -> wordcount in document -> log(P(w|c)) -> log(P(c)) -> log(p(w|c)) not in class
    val joinedWords = wordsAndCount.join(probabilityDataSet).where(0).equalTo(1) {
      (words, probabilityDataSet) => (probabilityDataSet._1, words._1, words._2, probabilityDataSet._3, probabilityDataSet._4, probabilityDataSet._5)
    }

    joinedWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/joinedWords.txt", WriteMode.OVERWRITE)

    //calculate sumpwc for found words
      // 1. Map: only needed information: class -> wordcount in document (each word) *  log(p(w|c)) each word
      // 2. Group-Reduce: sum log(p(w|c))
    val sumPwcFoundWords : DataSet[(String, Double)] = joinedWords.map(line => (line._1, line._3 * line._4))
      .groupBy(0)
      .reduce((line1, line2) => (line1._1, line1._2 + line2._2)) // class -> sum log(p(w|c)) for all found words

    sumPwcFoundWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/sumPwcFoundWords.txt", WriteMode.OVERWRITE)

    //calculate the amount of found words for each class
      // 1. Map: only needed information: class -> wordcount in document (each word) -> log(p(w|c)) not in class
      // 2. Group-Reduce: sum of wordcounts in document
      // 3. Map: calculate sumPwcNotFound: class -> (words.count -  sumwordcount) * log(p(w|c)) not in class
    val sumPwcNotFoundWords: DataSet[(String, Double)] = joinedWords.map(line => (line._1, line._3, line._6))
      .groupBy(0)
      .reduce((line1, line2) => (line1._1, line1._2 + line2._2, line1._3))
      .map(line => (line._1, (wordsInInput - line._2) * line._3))// class -> sum log(p(w|c)) for all not found words

    sumPwcNotFoundWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/sumPwcNotFoundWords.txt", WriteMode.OVERWRITE)

    //sum those pwc sums
      // 1. Join sumPwcFoundWords and sumPwcNotFoundWords
      // 2. Map: add sums from found words and sums from not found words
    val sumPwc: DataSet[(String, Double)] = sumPwcFoundWords.join(sumPwcNotFoundWords).where(0).equalTo(0) {
      (found, notfound) => (found._1, found._2 + notfound._2)
    }

    sumPwc.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/sumPwc.txt", WriteMode.OVERWRITE)

    //calculate possibility for each class
      // 1. only needed information: class -> log(p(c))
      // 2. Group-Reduce probabilitySet to one entry for each class
      // 3. Join sumPwc with probability DataSet to get log(P(c))
      // 4. Map: add sumPwc with log(P(c))
    val possibility: DataSet[(String, Double)] = probabilityDataSet.map(line => (line._1, line._4))
      .groupBy(0)
      .reduce((line1, line2) => (line1._1, line1._2))
      .join(sumPwc).where(0).equalTo(0) {
      (prob, pwc) => (prob._1, prob._2 + pwc._2)
      }

    possibility.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/possibility.txt", WriteMode.OVERWRITE)

    return null
  }
}

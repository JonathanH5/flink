package org.apache.flink.ml.classification

import java.lang.Iterable

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapPartitionFunction, FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.util.Collector
import scala.collection.mutable.Map


/**
 * Created by jonathanhasenburg on 05.05.15.
 */
class NormalNaiveBayesTwo extends Learner[(String, String), NormalNaiveBayesModelTwo] with Serializable {

 override def fit(input: DataSet[(String, String)], fitParameters: ParameterMap): NormalNaiveBayesModelTwo = {

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
   //val documentCount: Double = documentsPerClass.reduce((line1, line2) => (line1._1, line1._2 + line1._2)) // TODO -> aus documentsPerClass -> reduce and add
   val documentsCount: Double= input.count()

   // All words, but distinct
   val vocabulary = singleWordsInClass.map(tuple => (tuple._2, 1)).distinct(0)
   val vocabularyCount: Double = vocabulary.count

   println("Document count = " + documentsCount)
   println("Vocabulary count = " + vocabularyCount)

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

   return new NormalNaiveBayesModelTwo(probabilityDataSet)

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
   * The second string from the input gets split into its words, for each word a tuple is collected with the Int (count) 1
   */
  class SingleWordSplitter() extends FlatMapFunction[(String, String), (String, String, Int)] {
    override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {
      for (token: String <- value._2.split(" ")) {
        out.collect((value._1, token, 1))
      }
    }

    // Can be implemented via: input.flatMap{ pair => pair._2.split(" ").map{ token => (pair._1, token ,1)} }
  }

}

/**
 *
 * @param probabilityDataSet  Classname -> Word -> log(P(w|c)) -> log(P(c)) -> log(p(w|c)) not in class
 */
class NormalNaiveBayesModelTwo(probabilityDataSet: DataSet[(String, String, Double, Double, Double)]) extends Transformer[(Int, String), (Int, String)] {


  /**
   * Identifies the classes of an Input DataSet[ID -> Text] by creating an Output DataSet[ID -> Class]
   * @param input
   * @param transformParameters
   * @return
   */
  override def transform(input: DataSet[(Int, String)], transformParameters: ParameterMap): DataSet[(Int, String)] = {

    // Splits the texts from the input DataSet into its words
    val words : DataSet[(Int, String)] = input.flatMap {
      pair => pair._2.split(" ").map{ word => (pair._1, word)}
    }

    // genreate word counts for each word with a key
    // 1. Map: put a 1 to each key
    // 2. Group-Reduce: group by id and word and sum the 1s
    val wordsAndCount: DataSet[(Int, String, Int)] = words.map(line => (line._1, line._2, 1)).groupBy(0, 1).sum(2) // id -> word -> wordcount in text

    //calculate the count of all words for a text identified by its key
    val wordsInText: DataSet[(Int, Int)] = wordsAndCount.map(line => (line._1, line._3)).groupBy(0).sum(1) //id -> all words in text


    //join probabilityDataSet and words
    val joinedWords = wordsAndCount.join(probabilityDataSet).where(1).equalTo(1) {
      (words, probabilityDataSet) => (words._1, probabilityDataSet._1, words._2, words._3, probabilityDataSet._3, probabilityDataSet._4, probabilityDataSet._5)
    } // id -> classname -> Word -> wordcount in text (each word) -> log(P(w|c)) -> log(P(c)) -> log(p(w|c)) not in class

    joinedWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/joinedWords.txt", WriteMode.OVERWRITE)


    //calculate sumpwc for found words
    // 1. Map: only needed information: id -> class -> wordcount in text (each word) *  log(p(w|c)) each word
    // 2. Group-Reduce: sum log(p(w|c))
    val sumPwcFoundWords: DataSet[(Int, String, Double)] = joinedWords.map(line => (line._1, line._2 ,line._4 * line._5))
      .groupBy(0, 1)
      .reduce((line1, line2) => (line1._1, line1._2, line1._3 + line2._3)) // id -> class -> sum log(p(w|c)) for all found words in class

    sumPwcFoundWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/sumPwcFoundWords.txt", WriteMode.OVERWRITE)


    //calculate the amount of found words for each class
    // 1. Map: only needed information: id -> class -> wordcount in text (each word) -> log(p(w|c)) not in class
    // 2. Group-Reduce: sum of word counts in text
    // 3. Join with wordsInText to get the count of all words per text
    // 4. Map: calculate sumPwcNotFound: id -> class -> (word counts in text -  wordsInText) * log(p(w|c)) not in class
    val sumPwcNotFoundWords: DataSet[(Int, String, Double)] = joinedWords.map(line => (line._1, line._2, line._4, line._7))
      .groupBy(0, 1)
      .reduce((line1, line2) => (line1._1, line1._2, line1._3 + line2._3, line1._4))
      .join(wordsInText).where(0).equalTo(0) {
        (line1, line2) => (line1._1, line1._2, line1._3, line1._4, line2._2)
      }.map(line => (line._1, line._2, (line._5 - line._3) * line._4)) // id -> class -> sum log(p(w|c)) for all not found words

    sumPwcNotFoundWords.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/sumPwcNotFoundWords.txt", WriteMode.OVERWRITE)

    //sum those pwc sums
    // 1. Join sumPwcFoundWords and sumPwcNotFoundWords
    // 2. Map: add sums from found words and sums from not found words
    val sumPwc: DataSet[(Int, String, Double)] = sumPwcFoundWords.join(sumPwcNotFoundWords).where(0, 1).equalTo(0, 1) {
      (found, notfound) => (found._1, found._2, found._3 + notfound._3)
    } //id -> classname -> sum log(p(w|c))

    sumPwc.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/sumPwc.txt", WriteMode.OVERWRITE)


    //calculate possibility for each class
    // 1. only needed information: class -> log(p(c))
    // 2. Group-Reduce probabilitySet to one entry for each class
    // 3. Join sumPwc with probability DataSet to get log(P(c))
    // 4. Map: add sumPwc with log(P(c))
    val possibility: DataSet[(Int, String, Double)] = probabilityDataSet.map(line => (line._1, line._4))
      .groupBy(0)
      .reduce((line1, line2) => (line1._1, line1._2))
      .join(sumPwc).where(0).equalTo(1) {
        (prob, pwc) => (pwc._1, prob._1, prob._2 + pwc._3)
      }

    possibility.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/possibility.txt", WriteMode.OVERWRITE)

    val calculatedClasses: DataSet[(Int, String)] = possibility.groupBy(0).reduce(new CalculateReducer()).map(line => (line._1, line._2))

    return calculatedClasses

  }

  def saveModelDataSet(location: String) : Unit = {
    probabilityDataSet.writeAsCsv(location, "\n", "|", WriteMode.OVERWRITE)
  }

}

class CalculateReducer() extends ReduceFunction[(Int, String, Double)] {
  override def reduce(value1: (Int, String, Double), value2: (Int, String, Double)): (Int, String, Double) = {
    if (value1._3 > value2._3) {
      return value1;
    } else {
      return value2;
    }
  }
}

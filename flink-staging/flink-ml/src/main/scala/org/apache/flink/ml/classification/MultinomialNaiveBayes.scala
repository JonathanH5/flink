package org.apache.flink.ml.classification

import java.util

import org.apache.flink.api.common.functions.{RichMapFunction, ReduceFunction, FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.pipeline.{PredictDataSetOperation, FitOperation, Predictor}
import org.apache.flink.util.Collector


/**
 * While building the model different approaches need to be compared.
 * For that purpose the fitParameters are used. Every possibility that might enhance the implementation
 * can be chosen separately by using the following list of parameters:
 *
 * Possibility 1: document count
 *  P1 = 0 -> use .count() to get count of all documents
 *  P1 = 1 -> use a reducer and a mapper to create a broadcast data set containing the count of all documents
 *
 * Possibility 2: all words in class (order of operators)
 *  P2 = 0 -> first the reducer, than the mapper
 *  P2 = 1 -> first the mapper, than the reducer
 *
 * TODO Enhance
 */
class MultinomialNaiveBayes extends Predictor[MultinomialNaiveBayes] {

  import MultinomialNaiveBayes._

  //The model, that stores all needed information after the fitting phase
  var probabilityDataSet: Option[DataSet[(String, String, Double, Double, Double)]] = None

  // ============================== Parameter configuration =========================================

  def setP1(value: Int): MultinomialNaiveBayes = {
    parameters.add(P1, value)
    this
  }

  def setP2(value: Int): MultinomialNaiveBayes = {
    parameters.add(P1, value)
    this
  }

  // =============================================== Methods =========================================

  /**
   * Loads an already existing model created by the NaiveBayes algorithm. Requires the location
   * of the model. The saved model must be a representation of the [[probabilityDataSet]].
   * @param location, the location where the model should get stored
   */
  def saveModelDataSet(location: String) : Unit = {
    probabilityDataSet.get.writeAsCsv(location, "\n", "|", WriteMode.OVERWRITE)
  }

  /**
   * Sets the [[probabilityDataSet]] to the given data set.
   * @param loaded, the data set that gets loaded into the classifier
   */
  def setModelDataSet(loaded : DataSet[(String, String, Double, Double, Double)]) : Unit = {
    this.probabilityDataSet = Some(loaded)
  }

  //Configuration options
  //TODO setPossibility Method and more possiblities


}

object MultinomialNaiveBayes {

   // ======================================== Factory Methods =====================================

  def apply(): MultinomialNaiveBayes = {
    new MultinomialNaiveBayes()
  }

  // ========================================== Parameters =========================================

  case object P1 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(0)
  }

  case object P2 extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  // ====================================== Operations =============================================

  /**
   * Trains the model to fit the training data. The resulting [[MultinomialNaiveBayes.probabilityDataSet]] is stored in
   * the [[MultinomialNaiveBayes]] instance.
   */


  implicit val fitNNB = new FitOperation[MultinomialNaiveBayes, (String, String)] {
    /**
     * The [[FitOperation]] used to create the model. Requires an instance of [[MultinomialNaiveBayes]], a [[ParameterMap]]
     * and the input data set. This data set maps (string -> string) containing (label -> text, words separated by ",")
     * @param instance
     * @param fitParameters
     * @param input
     */
    override def fit(instance: MultinomialNaiveBayes,
                     fitParameters: ParameterMap,
                     input: DataSet[(String, String)]): Unit = {

      /**
       * Count the amount of documents for each class.
       * 1. Map: replace the document text by a 1
       * 2. Group-Reduce: sum the 1s by class
       */
      val documentsPerClass: DataSet[(String, Int)] = input.map { input => (input._1, 1)}
        .groupBy(0).sum(1) // (class name -> count of documents)

      /**
       * Count the amount of occurrences of each word for each class.
       * 1. FlatMap: split the document into its words and add a 1 to each tuple
       * 2. Group-Reduce: sum the 1s by class, word
       */
      val singleWordsInClass: DataSet[(String, String, Int)] = input.flatMap(new SingleWordSplitter())
        .groupBy(0, 1).sum(2) // (class name -> word -> count of that word)


      // TODO Possibility 2 (first reduce than map or the other way around)
      val allWordsInClass: DataSet[(String, Int)] = singleWordsInClass.groupBy(0).reduce {
        (singleWords1, singleWords2) => (singleWords1._1, singleWords1._2, singleWords1._3 + singleWords2._3)
      }.map(singleWords => (singleWords._1, singleWords._3)) // (class name -> count of all words in that class)

      // (class name -> word -> count of that word -> count of all words in class)
      val wordsInClass = singleWordsInClass.join(allWordsInClass).where(0).equalTo(0) {
        (single, all) => (single._1, single._2, single._3, all._2)
      }

      // (count of all documents)
      // TODO Possibility 1
      val documentsCount2: DataSet[(Double)] = documentsPerClass.reduce((line1, line2) => (line1._1, line1._2 + line2._2)).map(line => line._2)
      val documentsCount: Double = input.count()

      // (list of all words, but distinct)
      val vocabulary = singleWordsInClass.map(tuple => (tuple._2, 1)).distinct(0)
      // (count of items in vocabulary list)
      val vocabularyCount: Double = vocabulary.count()

      println("Document count = " + documentsCount)
      println("Vocabulary count = " + vocabularyCount)

      //******************************************************************************************************************
      //calculate P(w) and P(w|c)

      // Classname -> P(w) in class
      // TODO POSSIBILITY 1
      val pw: DataSet[(String, Double)] = documentsPerClass.map(line => (line._1, line._2 / documentsCount))
      // TODO Possibility 1
      val pw2: DataSet[(String, Double)] = documentsPerClass.map(new RichMapFunction[(String, Int), (String, Double)] {

        var broadcastSet: util.List[Double] = null

        override def open(config: Configuration): Unit = {
          broadcastSet = getRuntimeContext.getBroadcastVariable[Double]("documentCount")
          //TODO Test size of broadCast Set
        }

        override def map(value: (String, Int)): (String, Double) = {
          (value._1, value._2 / broadcastSet.get(0))
        }
      }).withBroadcastSet(documentsCount2, "documentCount")


      // Classname -> P(w|c) word not in class
      val pwcNotInClass: DataSet[(String, Double)] = allWordsInClass.map(line => (line._1, 1 / (line._2 + vocabularyCount)))
      pwcNotInClass.writeAsText("/Users/jonathanhasenburg/Desktop/naiveB/pwcNotInClass.txt", WriteMode.OVERWRITE)

      // Classname -> p(c) -> p(w|c) not in class
      val classInfo = pw.join(pwcNotInClass).where(0).equalTo(0) {
        (line1, line2) => (line1._1, line1._2, line2._2)
      }

      // Classname -> Word -> P(w|c)
      val pwc: DataSet[(String, String, Double)] = wordsInClass.map(line => (line._1, line._2, ((line._3 + 1) / (line._4 + vocabularyCount))))

      // Classname -> Word -> log(P(w|c)) -> log(P(c)) -> log(p(w|c)) not in class
      val probabilityDataSet = pwc.join(classInfo).where(0).equalTo(0) {
        (pwc, classInfo) => (pwc._1, pwc._2, Math.log(pwc._3), Math.log(classInfo._2), Math.log(classInfo._3))
      }

      instance.probabilityDataSet = Some(probabilityDataSet)
    }
  }

  // Model (String, String, Double, Double, Double)

  implicit def predictNNB = new PredictDataSetOperation[MultinomialNaiveBayes, (Int, String), (Int, String)]() {

    override def predictDataSet(instance: MultinomialNaiveBayes,
                                predictParameters: ParameterMap,
                                input: DataSet[(Int, String)]): DataSet[(Int, String)] = {

      if (instance.probabilityDataSet.equals(None)) {
        throw new RuntimeException("The NormalNaiveBayes has not been fitted to the " +
            "data. This is necessary before a prediction on other data can be made.")
      }

      val probabilityDataSet = instance.probabilityDataSet.get;

      // Splits the texts from the input DataSet into its words
      val words: DataSet[(Int, String)] = input.flatMap {
        pair => pair._2.split(" ").map { word => (pair._1, word)}
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

      //calculate sumpwc for found words
      // 1. Map: only needed information: id -> class -> wordcount in text (each word) *  log(p(w|c)) each word
      // 2. Group-Reduce: sum log(p(w|c))
      val sumPwcFoundWords: DataSet[(Int, String, Double)] = joinedWords.map(line => (line._1, line._2, line._4 * line._5))
        .groupBy(0, 1)
        .reduce((line1, line2) => (line1._1, line1._2, line1._3 + line2._3)) // id -> class -> sum log(p(w|c)) for all found words in class

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

      //sum those pwc sums
      // 1. Join sumPwcFoundWords and sumPwcNotFoundWords
      // 2. Map: add sums from found words and sums from not found words
      val sumPwc: DataSet[(Int, String, Double)] = sumPwcFoundWords.join(sumPwcNotFoundWords).where(0, 1).equalTo(0, 1) {
        (found, notfound) => (found._1, found._2, found._3 + notfound._3)
      } //id -> classname -> sum log(p(w|c))

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

      //That is the return
      possibility.groupBy(0).reduce(new CalculateReducer()).map(line => (line._1, line._2))



    }
  }



  /*
  * ******************************************************************************************************************
  * *******************************************Function Classes*******************************************************
  * ******************************************************************************************************************
   */

  /**
   * Transforms a (String, String) tuple into a (String, String, Int)) tuple.
   * The second string from the input gets split into its words, for each word a tuple is collected with the Int 1.
   */
  class SingleWordSplitter() extends FlatMapFunction[(String, String), (String, String, Int)] {
    override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {
      for (token: String <- value._2.split(" ")) {
        out.collect((value._1, token, 1))
      }
    }

    // Can be implemented via: input.flatMap{ pair => pair._2.split(" ").map{ token => (pair._1, token ,1)} }
  }

  /**
   * Chooses for each label that class with the highest value
   */
  class CalculateReducer() extends ReduceFunction[(Int, String, Double)] {
    override def reduce(value1: (Int, String, Double), value2: (Int, String, Double)): (Int, String, Double) = {
      if (value1._3 > value2._3) {
        return value1;
      } else {
        return value2;
      }
    }
  }

}

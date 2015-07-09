package org.apache.flink.ml.classification

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions.{RichMapFunction, ReduceFunction, RichMapPartitionFunction, FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.pipeline.{PredictOperation, FitOperation, Predictor}
import org.apache.flink.util.Collector
import scala.collection.mutable.Map


class NormalNaiveBayes extends Predictor[NormalNaiveBayes] {

  //The model, that stores all needed information after the fitting phase
  var probabilityDataSet: Option[DataSet[(String, String, Double, Double, Double)]] = None

  //Configuration options

  //TODO setPossibility Method and more possiblities

  /*
    While building the model different approaches need to get benchmarked.
    For that purpose the fitParameters are used. Every possiblity in the code to choose one or option or
    another can be chocen by using these parameters.

      p1 = 0 -> use .count() to get amount of all documents
      p1 = 1 -> use a ... to get amount of all documents

   */

}

object NormalNaiveBayes {

   // ======================================== Factory methods ======================================

  def apply(): NormalNaiveBayes = {
    new NormalNaiveBayes()
  }

  // ====================================== Operations =============================================

  /**
   * Trains the model to fit the training data. The resulting possibilityDataSet is stored in
   * the [[NormalNaiveBayes]] instance.
   */

  implicit val fitNNB = new FitOperation[NormalNaiveBayes, (String, String)] {
    override def fit(instance: NormalNaiveBayes,
                     fitParameters: ParameterMap,
                     input: DataSet[(String, String)]): Unit = {

      // Classname -> Count of documents
      val documentsPerClass: DataSet[(String, Int)] = input.map { line => (line._1, 1)}
        .groupBy(0)
        .sum(1)

      // Classname -> Word -> Count of that word
      val singleWordsInClass: DataSet[(String, String, Int)] = input.flatMap(new SingleWordSplitter())
        .groupBy(0, 1)
        .sum(2)

      // Classname -> Count of all words in class
      val allWordsInClass: DataSet[(String, Int)] = singleWordsInClass.groupBy(0).reduce {
        (line1, line2) => (line1._1, line1._2, line1._3 + line2._3)
      }.map(line => (line._1, line._3))

      // Classname -> Word -> Count of that word -> Count of all words in class
      val wordsInClass = singleWordsInClass.join(allWordsInClass).where(0).equalTo(0) {
        (single, all) => (single._1, single._2, single._3, all._2)
      }

      // Count of all documents POSSIBILITY 1
      val documentsCount2: DataSet[(Double)] = documentsPerClass.reduce((line1, line2) => (line1._1, line1._2 + line2._2)).map(line => line._2) // TODO -> aus documentsPerClass -> reduce and add
      val documentsCount: Double = input.count()

      // All words, but distinct
      val vocabulary = singleWordsInClass.map(tuple => (tuple._2, 1)).distinct(0)
      val vocabularyCount: Double = vocabulary.count

      println("Document count = " + documentsCount)
      println("Vocabulary count = " + vocabularyCount)

      //******************************************************************************************************************
      //calculate P(w) and P(w|c)

      // Classname -> P(w)  in class POSSIBILITY 1
      val pw: DataSet[(String, Double)] = documentsPerClass.map(line => (line._1, line._2 / documentsCount))
      pw.print()
      val pw2: DataSet[(String, Double)] = documentsPerClass.map(new RichMapFunction[(String, Int), (String, Double)] {

        var broadcastSet: util.List[Double] = null

        override def open(config: Configuration): Unit = {
          broadcastSet = getRuntimeContext.getBroadcastVariable[Double]("documentCount")
          //TODO Test size of broadCast Set
        }

        override def map(value: (String, Int)): (String, Double) = {
          return (value._1, value._2 / broadcastSet.get(0))
        }
      }).withBroadcastSet(documentsCount2, "documentCount")

      pw2.print()

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

  implicit def predictNNB[inputD : DataSet[(Int, String)] = {
    new PredictOperation[NormalNaiveBayes, (String, String, Double, Double, Double), inputD, DataSet[(Int, String)]]() {
      /** Defines how to retrieve the model of the type for which this operation was defined
        *
        * @param instance The Predictor instance that we will use to make the predictions
        * @param predictParameters The parameters for the prediction
        * @return A DataSet with the model representation as its only element
        */
      override def getModel(instance: NormalNaiveBayes, predictParameters: ParameterMap): DataSet[(String, String, Double, Double, Double)] = {
        self.probabilityDataset match {
          case Some(weights) => probabilityDataset


          case None => {
            throw new RuntimeException("The NormalNaiveBayes has not been fitted to the " +
              "data. This is necessary before a prediction on other data can be made.")
          }
        }
      }

      /** Calculates the prediction for a single element given the model of the [[Predictor]].
        *
        * @param input The input dataset, Int -> String (ID -> Text)
        * @param probabilityDataSet The model representation based on a fitted train set
        * @return The output dataset, Int -> String (ID -> Text)
        */
      override def predict(input: inputD, probabilityDataSet: DataSet[(String, String, Double, Double, Double)]): DataSet[(Int, String)] = {

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

        //calculate sumpwc for found words
        // 1. Map: only needed information: id -> class -> wordcount in text (each word) *  log(p(w|c)) each word
        // 2. Group-Reduce: sum log(p(w|c))
        val sumPwcFoundWords: DataSet[(Int, String, Double)] = joinedWords.map(line => (line._1, line._2 ,line._4 * line._5))
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

        val calculatedClasses: DataSet[(Int, String)] = possibility.groupBy(0).reduce(new CalculateReducer()).map(line => (line._1, line._2))

        //TODO -> Wie "liefere" ich das Ergebnis?
      }
    }

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

/* OLD Code


/**
 *
 * @param probabilityDataSet  Classname -> Word -> log(P(w|c)) -> log(P(c)) -> log(p(w|c)) not in class
 */
class NormalNaiveBayesModel(probabilityDataSet: DataSet[(String, String, Double, Double, Double)]) extends Transformer[(Int, String), (Int, String)] {


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

    //calculate sumpwc for found words
    // 1. Map: only needed information: id -> class -> wordcount in text (each word) *  log(p(w|c)) each word
    // 2. Group-Reduce: sum log(p(w|c))
    val sumPwcFoundWords: DataSet[(Int, String, Double)] = joinedWords.map(line => (line._1, line._2 ,line._4 * line._5))
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

    val calculatedClasses: DataSet[(Int, String)] = possibility.groupBy(0).reduce(new CalculateReducer()).map(line => (line._1, line._2))

    return calculatedClasses
  }

  def saveModelDataSet(location: String) : Unit = {
    probabilityDataSet.writeAsCsv(location, "\n", "|", WriteMode.OVERWRITE)
  }

}

}


*/

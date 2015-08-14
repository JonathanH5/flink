/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.classification

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.JavaConverters._


class CRQSelection extends Transformer[CRQSelection]{

  import CRQSelection._

  // The data needed to transform based on the crq model
  //(class -> document id -> word -> word frequency -> P_i(w_t|d_i) -> P_j(w_t|d_i) -> P(w_t))
  var crqModel: Option[DataSet[(String, Int, String, Int, Double, Double, Double)]] = None

  def setSaveLocationCategories(value: String): CRQSelection = {
    parameters.add(saveLocationCategories, value)
    this
  }

}

object CRQSelection {

  // ====================================== Parameters =============================================

  /**
   * Save location for the data set storing the
   * correct categories from the transformed predcit data set and their document id.
   */
  case object saveLocationCategories extends Parameter[String] {
    override val defaultValue: Option[String] = None
  }

  // ==================================== Factory methods ==========================================

  def apply(): CRQSelection = {
    new CRQSelection()
  }

  // ====================================== Operations =============================================

  implicit val crqFit = new FitOperation[CRQSelection, (String, Int, String)] {
    /**
     * Creates the [[CRQSelection.crqModel]] needed for the transformation.
     * @param instance of [[CRQSelection]]
     * @param fitParameters to configure
     * @param input, the input data set to train the model
     */
    override def fit(instance: CRQSelection,
                     fitParameters: ParameterMap,
                     input: DataSet[(String, Int, String)]): Unit = {

      //Create a data set that stores all word related information
      // 1. Map: Split documents into its words and add a 1 to each tuple
      // 2. Group-Reduce: sum the 1s by class and document id
      val wordRelated: DataSet[(String, Int, String, Int)] = input.flatMap(new SingleWordSplitter)
        .groupBy(0, 1)
        .sum(3) // (class -> document id -> word -> word frequency)

      //Create a data set that stores all document related information
      // 1. Map: remove the word and class information
      // 2. Reduce: sum the word counts in each document
      val docRelated: DataSet[(Int, Int)] = wordRelated.map(line => (line._2, line._4)).reduce((line1, line2)
      => (line1._1, line1._2 + line2._2)) // (document id -> total word frequency)

      //Create the crqModel dataset
      // 1. Map: calculate P_i(w_t|d_i)
      val output: DataSet[(String, Int, String, Int, Double)] = wordRelated
        .map(new RichMapFunction[(String, Int, String, Int),
        (String, Int, String, Int, Double)] {

        var broadcastMap: mutable.Map[Int, Double] = mutable.Map[Int, Double]()

        override def open(config: Configuration): Unit = {
          val collection = getRuntimeContext
            .getBroadcastVariable[(Int, Int)]("docRelated").asScala
          for (record <- collection) {
            broadcastMap.put(record._1, record._2.toDouble)
          }
        }

        override def map(value: (String, Int, String, Int)): (String, Int, String, Int, Double) = {
          (value._1, value._2, value._3, value._4, value._4 / broadcastMap(value._2))
        }
      }).withBroadcastSet(docRelated, "docRelated")


      output.writeAsCsv("/Users/jonathanhasenburg/Desktop/nbtemp/output",
        "\n", "\t", WriteMode.OVERWRITE)


    }
  }

  implicit val crqTransformFit = new TransformDataSetOperation[CRQSelection,
    (String, Int, String), (String, String)] {
    /**
     * Uses the [[CRQSelection.crqModel]] to transform an input data set into a data set in the
     * format that is required by the [[MultinomialNaiveBayes]] to fit.
     * @param instance of [[CRQSelection]]
     * @param transformParameters to configure
     * @param input, the input data set that is transformed
     * @return
     */
    override def transformDataSet(instance: CRQSelection,
                                  transformParameters: ParameterMap,
                                  input: DataSet[(String, Int, String)]):
                                    DataSet[(String, String)] = {
      return null
    }
  }

  implicit val crqTransformPredict = new TransformDataSetOperation[CRQSelection,
    (String, Int, String), (Int, String)] {
    /**
     * Uses the [[CRQSelection.crqModel]] to transform an input data set into a data set in the
     * format that is required by the [[MultinomialNaiveBayes]] to predict.
     * @param instance of [[CRQSelection]]
     * @param transformParameters to configure
     * @param input, the input data set that is transformed (class -> document id -> document text)
     * @return,
     */
    override def transformDataSet(instance: CRQSelection,
                                  transformParameters: ParameterMap,
                                  input: DataSet[(String, Int, String)]): DataSet[(Int, String)] = {
      return null
    }
  }

  private def selectFeatures(input: DataSet[(String, Int, String)])
          : DataSet[(String, Int, String)] = {
    return null


  }

  /*
* ************************************************************************************************
* *******************************************Function Classes*************************************
* ************************************************************************************************
 */

  /**
   * Transforms a (String, Int, String) tuple into a (String, Int, String, Int)) tuple.
   * The third element from the input gets split into its words, for each word a tuple is collected
   * with the Int 1.
   */
  class SingleWordSplitter() extends FlatMapFunction[(String, Int, String), (String, Int, String, Int)] {
    override def flatMap(value: (String, Int, String),
                         out: Collector[(String, Int, String, Int)]): Unit = {
      for (token: String <- value._3.split(" ")) {
        out.collect((value._1, value._2, token, 1))
      }
    }
  }

}

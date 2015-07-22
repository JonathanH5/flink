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

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class MultinomialNaiveBayesRuns extends FlatSpec with Matchers with FlinkTestBase {

  val dataSetFolder = "/Users/jonathanhasenburg/OneDrive/datasets/bbc"
  val runNumber = 3

  behavior of "The MultinomialNaiveBayes implementation"

  it should "train the classifier with the basic configuration" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes()

    val trainingDS = env.readCsvFile[(String, String)](dataSetFolder + "/input/train.csv", "\n", "\t")
    nnb.fit(trainingDS)
    nnb.saveModelDataSet(dataSetFolder + "/runs/run"+runNumber+"/basicConfigModel.csv")

    env.execute()
  }

it should "train the classifier with p1 = 0" in {
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val nnb = MultinomialNaiveBayes().setP1(0)

  val trainingDS = env.readCsvFile[(String, String)](dataSetFolder + "/input/train.csv", "\n", "\t")
  nnb.fit(trainingDS)
  nnb.saveModelDataSet(dataSetFolder + "/runs/run"+runNumber+"/p1_0.csv")

  env.execute()
}

it should "train the classifier with p1 = 1" in {
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val nnb = MultinomialNaiveBayes().setP1(1)

  val trainingDS = env.readCsvFile[(String, String)](dataSetFolder + "/input/train.csv", "\n", "\t")
  nnb.fit(trainingDS)
  nnb.saveModelDataSet(dataSetFolder + "/runs/run"+runNumber+"/p1_1.csv")

  env.execute()
}


  /* WORKS
  it should "train the classifier with p2 = 0" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes().setP2(0)

    val trainingDS = env.readCsvFile[(String, String)](dataSetFolder + "/input/train.csv", "\n", "\t")
    nnb.fit(trainingDS)
    nnb.saveModelDataSet(dataSetFolder + "/runs/run"+runNumber+"/p2_0.csv")

    env.execute()
  }

  it should "train the classifier with p2 = 1" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val nnb = MultinomialNaiveBayes().setP2(1)

    val trainingDS = env.readCsvFile[(String, String)](dataSetFolder + "/input/train.csv", "\n", "\t")
    nnb.fit(trainingDS)
    nnb.saveModelDataSet(dataSetFolder + "/runs/run"+runNumber+"/p2_1.csv")

    env.execute()
  }
  */

  it should "use the basicConfigModel model to predict" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val modelSet = env.readCsvFile[(String, String, Double, Double, Double)](dataSetFolder + "/runs/run" +
      runNumber + "/basicConfigModel.csv", "\n", "|")

    val nnb = MultinomialNaiveBayes()
    nnb.setModelDataSet(modelSet)

    val solution = nnb.predict(env.readCsvFile[(Int, String)](dataSetFolder + "/input/test.csv", "\n", "\t"))

    solution.writeAsCsv(dataSetFolder + "/runs/run" + runNumber + "/computedCategories.csv", "\n", "\t", WriteMode.OVERWRITE)

    env.execute()

  }

}

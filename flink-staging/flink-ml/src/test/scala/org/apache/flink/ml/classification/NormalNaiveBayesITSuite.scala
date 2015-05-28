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

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class NormalNaiveBayesITSuite extends FlatSpec with Matchers with FlinkTestBase {

  val saveLocationModel = "/Users/jonathanhasenburg/Desktop/naiveB/model.csv"

  behavior of "The NormalNaiveBayes implementation"

  it should "train a NaiveBayesClassifier" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val learner = new NormalNaiveBayesTwo()
    //val trainingDS = env.readCsvFile[(String, String)]("/Users/jonathanhasenburg/Desktop/naiveB/testfile2.txt", "\n", "\t")
    val trainingDS = env.readCsvFile[(String, String)]("/Users/jonathanhasenburg/Desktop/bayes/bbcTrain.csv", "\n", "\t")


    val model = learner.fit(trainingDS)

    model.saveModelDataSet(saveLocationModel)


    //model.transform(env.fromElements("and this is god belief belief it or not correct is what you think tragic tragedy is not what god want"))
   // model.transform(env.fromElements((1, "christian moral articl eastman dp nasa kodak write articl vice ico tek bobb vice ico tek robert beauchain write not believ god not concern disposit beneath provid evid requir evid that person thi god find compel fact god you you for minut you love you you made love you want love you don expect love don even exist secondli wouldn expect love simpli creator expect earn that love respons you love god and step promis for you you for you daft love don exist come back you learn love your testicl doubt thi disput not givin sincer effort simpl logic argument folli you read bibl you will that jesu made fool trick logic abil reason spec creation ultim you reli simpli your reason you will never you learn you accept that you don point you step line and becom complet asshol even your offens won slip becuas heard goddamn time you love jesu deep your heart you cannibalist necrophiliac and qualifi assess your motiv you fortun thing accept evid faith that christian quit fuck arrog will never peac you made that bob beauchain bobb vice ico tek that queen stai blew bronx and sank manhattan sea")))

    env.execute()

    /*
    val model = learner.fit(trainingDS)

    val weightVector = model.weights.collect().apply(0)

    weightVector.valuesIterator.zip(Classification.expectedWeightVector.valueIterator).foreach {
      case (weight, expectedWeight) =>
        weight should be(expectedWeight +- 0.1)
    }
    */
  }

  it should "transform an input string" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val modelSet = env.readCsvFile[(String, String, Double, Double, Double)](saveLocationModel, "\n", "|")

    val model = new NormalNaiveBayesModelTwo(modelSet)

    //val solution = model.transform(env.fromElements((1, "christian moral articl eastman dp nasa kodak write articl vice ico tek bobb vice ico tek robert beauchain write not believ god not concern disposit beneath provid evid requir evid that person thi god find compel fact god you you for minut you love you you made love you want love you don expect love don even exist secondli wouldn expect love simpli creator expect earn that love respons you love god and step promis for you you for you daft love don exist come back you learn love your testicl doubt thi disput not givin sincer effort simpl logic argument folli you read bibl you will that jesu made fool trick logic abil reason spec creation ultim you reli simpli your reason you will never you learn you accept that you don point you step line and becom complet asshol even your offens won slip becuas heard goddamn time you love jesu deep your heart you cannibalist necrophiliac and qualifi assess your motiv you fortun thing accept evid faith that christian quit fuck arrog will never peac you made that bob beauchain bobb vice ico tek that queen stai blew bronx and sank manhattan sea")))

    //val solution = model.transform(env.readCsvFile[(Int, String)]("/Users/jonathanhasenburg/Desktop/naiveB/testfile3.txt", "\n", "\t"))

    val solution = model.transform(env.readCsvFile[(Int, String)]("/Users/jonathanhasenburg/Desktop/bayes/bbcTest.csv", "\n", "\t"))

    solution.writeAsCsv("/Users/jonathanhasenburg/Desktop/bayes/bbcTestComputedCategories.csv", "\n", "\t", WriteMode.OVERWRITE)

    env.execute()

  }
}

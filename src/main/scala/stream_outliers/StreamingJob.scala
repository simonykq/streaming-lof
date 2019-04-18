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

package stream_outliers

import java.util.Iterator

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {

  final val WINDOW_SIZE = 1000

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // the host and the port to connect to
    var inputFile = "dataset/kddcup.data"
    var kind = "KddCup99"

    val attacks = "mscan.\nwarezmaster.\nsnmpgetattack.\nback.\nmailbomb.\nguess_passwd.\nsatan.\nsnmpguess."
    val covTypes = List("1", "2", "3", "4")

    try {
      val params = ParameterTool.fromArgs(args)
      inputFile = if (params.has("inputFile")) params.get("inputFile") else inputFile
      kind = if (params.has("kind")) params.get("kind") else kind
    } catch {
      case e: Exception =>
        System.err.println("No inputFile specified.")
        return
    }

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stream = env.readTextFile(inputFile)

    val window = stream
//        .filter { _.nonEmpty }
        .filter { w => !attacks.split("\n").contains(w.split(",").last)}
//          .filter { w => covTypes.contains(w.split(",").last) }
           .map { Dataset(_, kind) }
//        .timeWindowAll(Time.milliseconds(200))
        .countWindowAll(WINDOW_SIZE)
        .apply(new CKdeWR()).setParallelism(1)

    window.print()

//    val finalResult = DataStreamUtils.collect(window.javaStream).asScala.foldLeft(Summary[TPoint]())(_ + _)
    val finalResult = DataStreamUtils.collect(window.javaStream).asScala.reduce(_ + _)

    println("C_KDE_WR on CovType")
//    println("SOD_GPU on CovType")

//    println("C_LOF on Syntactic 2")

    println(s"True Positives: ${finalResult.tp}")
    println(s"False Positives: ${finalResult.fp}")
    println(s"True Negatives: ${finalResult.tn}")
    println(s"False Negatives: ${finalResult.fn}")

    println(s"Accuracy: ${finalResult.accuracy}")
    println(s"Sensitivity: ${finalResult.sensitivity}")
    println(s"Specificity: ${finalResult.specificity}")
    println(s"Precision: ${finalResult.precision}")
    println(s"F-Score: ${finalResult.f_score}")

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")

  }

}

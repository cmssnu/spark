/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.reef

import java.io.File
import java.util.concurrent.Executors

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class REEFDriverDelegateSuite extends FunSuite with MockitoSugar {

  test("test setSystemPropertiesFromFile empty") {
    val file = mock[File]
    intercept[IllegalArgumentException] {
      REEFDriverDelegate.setSystemPropertiesFromFile(file)
    }
  }

  test("test startUserClass mock class") {
    val userClass = "org.apache.spark.Mock"
    val userJar = Array("mock1.jar", "mock2.jar", "mock3.jar")
    intercept[RuntimeException] {
      REEFDriverDelegate.startUserClass(userClass, new Array[String](0), userJar)
    }
  }

  test("test waitForREEFEvaluatorInit") {
    val t = new Thread {
      override def run() {
        val conf = new SparkConf().setMaster("reef").setAppName("mock")
        val sc = new SparkContext(conf)
      }
    }
    val executor = Executors.newSingleThreadExecutor()
    val status = executor.submit(t)
    assert(status.isCancelled === false)
    assert(REEFDriverDelegate.isDoneWithREEFEvaluatorInit === false)
  }

}
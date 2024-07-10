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

package org.apache.spark.exp

import java.io.File
import java.io.FileWriter
import java.time.Duration
import java.time.Instant

import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.HadoopFsRelation



/*
 * List of config parameters:
 * dryRunIters: # of iterations for dry run before the main experiment
 * experimentIters: # of iterations for the main experiment
 * deltaDBName: Database name of DeltaLake database
 * deltaTableName: Table name of DeltaLake table
 * hmsDBName: Database name of HMS database
 * hmsTableName: Table name of HMS table
 */
object Experiment1 {

  def main(args: Array[String]): Unit = {
    if (args.size != 1) {
      print("Usage: spark-class org.apache.spark.exp.Experiment1 <expConfig>\n")
      return
    }

    // read expConfig into map
    val jsonParser = new ObjectMapper
    val expConfig = scala.collection.mutable.Map.empty[String, String]
    jsonParser.readTree(Source.fromFile(args(0)).mkString).fields().forEachRemaining { config =>
      expConfig.put(config.getKey, config.getValue.asText())
    }

    val resultOutput = expConfig.getOrElse("resultOutput", "/tmp/experiment1.txt")
    val experimentIters = expConfig.getOrElse("experimentIters", "10").toInt
    val deltaDBName = expConfig.getOrElse("deltaDBName", "experiment1delta")
    val deltaTableName = expConfig.getOrElse("deltaTableName", "table1")
    val hmsDBName = expConfig.getOrElse("hmsDBName", "experiment1hms")
    val hmsTableName = expConfig.getOrElse("hmsTableName", "table1")
    val treeAddress = expConfig.getOrElse("treeAddress", "localhost:9876")

    val expUtil = new ExperimentUtil(treeAddress)
    val outputWriter = new FileWriter(new File(resultOutput),true)
    outputWriter.write("Tree,HMS,DeltaLake\n")

    for (i <- 0 until experimentIters) {
      // list files, using TreeCatalog
      val treeStartTime = Instant.now()
      val treeFiles = expUtil.tree.listFiles(hmsDBName, hmsTableName, None)
      val treeEndTime = Instant.now()
      val treeTime = Duration.between(treeStartTime, treeEndTime).toNanos()

      // list files, using HMS
      val hmsStartTime = Instant.now()
      val hmsPartitions = expUtil.hms.listPartitions(hmsDBName, hmsTableName)
      hmsPartitions.foreach { partition =>
        val hmsFiles = expUtil.hms_ext.listFiles(partition)
      }
      val hmsEndTime = Instant.now()
      val hmsTime = Duration.between(hmsStartTime, hmsEndTime).toNanos()

      // list files, using DeltaLake
      val deltaStartTime = Instant.now()
      val table = expUtil.delta.loadTable(Identifier.of(Array(deltaDBName), deltaTableName))
      val baseRelation = table.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(Seq.empty, Seq.empty)
      val deltaEndTime = Instant.now()
      val deltaTime = Duration.between(deltaStartTime, deltaEndTime).toNanos()
      outputWriter.write(treeTime + "," + hmsTime + "," + deltaTime + "\n")
      outputWriter.flush()
    }
    outputWriter.close()

  }

}

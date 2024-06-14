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

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.math3.random.RandomDataGenerator

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.HadoopFsRelation


/*
 * List of config parameters:
 * dryRunIters: # of iterations for dry run before the main experiment
 * experimentIters: # of iterations for the main experiment
 * deltaDBName: Database name of DeltaLake database
 * deltaTableName: Table name of DeltaLake table
 * hmsDBName: Database name of HMS database
 * hmsTableName: Table name of HMS table
 * partitionRange: Min and max of the partition range to be scanned (from uniform distribution)
 */

object Experiment2 {
  private val sqlParser = new SparkSqlParser()
  private var hmsTablejson : JsonNode = _

  def main(args: Array[String]): Unit = {
    if (args.size != 3) {
      print("Usage: spark-class org.apache.spark.exp.Experiment1 <hmsconfig> " +
        "<deltaconfig> <expConfig>\n")
      return
    }

    val jsonParser = new ObjectMapper
    // separate configuration for HMS and Delta tables, including database name, table schema etc.
    val hmsConfig = jsonParser.readTree(Source.fromFile(args(0)).mkString)
    val deltaConfig = jsonParser.readTree(Source.fromFile(args(1)).mkString)
    // experiment configuration
    val expConfig = scala.collection.mutable.Map.empty[String, String]
    jsonParser.readTree(Source.fromFile(args(2)).mkString).fields().forEachRemaining { config =>
      expConfig.put(config.getKey, config.getValue.asText())
    }

    val dryRunIters = expConfig.getOrElse("dryRunIters", "4").toInt
    val experimentIters = expConfig.getOrElse("experimentIters", "10").toInt
    val partitionRange = expConfig.getOrElse("partitionRange", "1:30").split(":").map(_.toInt)
    val treeAddress = expConfig.getOrElse("treeAddress", "localhost:9876")
    val hmsDBName = hmsConfig.get("databaseName").asText()
    hmsTablejson = hmsConfig.get("tables").get(0)
    val hmsTableName = hmsTablejson.get("name").asText()
    val deltaDBName = deltaConfig.get("databaseName").asText()
    val deltaTablejson = deltaConfig.get("tables").get(0)
    val deltaTableName = deltaTablejson.get("name").asText()

    var treeTime: Long = 0
    var hmsTime: Long = 0
    var deltaTime: Long = 0

    val expUtil = new ExperimentUtil(treeAddress)

    val dataGen = new RandomDataGenerator()
    dataGen.reSeed(1)
    for (i <- 0 until dryRunIters) {
      // construct partition vals
      // the schema for HMS table and delta table should be the same!
      val partitionValsSeq = ArrayBuffer[List[Int]]()
      hmsTablejson.get("partitionSchema").forEach { partitionCol =>
        if (partitionCol.get("type").asText() == "INT") {
          val partitionMax = partitionCol.get("max").asInt()
          val rangeMin = dataGen.nextInt(1, partitionMax)
          val rangeMax = math.min(dataGen.nextInt(partitionRange(0), partitionRange(1))
            + rangeMin, partitionMax)
          partitionValsSeq.append(List(rangeMin, rangeMax))
        }
        else if (partitionCol.get("type").asText() == "DATE") {
          val minDate = LocalDate.parse(partitionCol.get("min").asText())
          val maxDate = LocalDate.parse(partitionCol.get("max").asText())
          val numDays = (ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt
          val rangeMin = dataGen.nextInt(1, numDays)
          val rangeMax = math.min(dataGen.nextInt(partitionRange(0), partitionRange(1))
            + rangeMin, numDays)
          partitionValsSeq.append(List(rangeMin, rangeMax))
        }
      }
      // the same filters can also be used for delta table
      val hmsFilters = constructHMSFilters(partitionValsSeq)
      val treeFilters = constructTreeFilters(partitionValsSeq)

      // list files, using TreeCatalog
      val treeFiles = expUtil.tree.listFilesByFilter(hmsDBName, hmsTableName, treeFilters, None)

      // list files, using HMS
      val hmsPartitions = expUtil.hms.listPartitionsByFilter(hmsDBName, hmsTableName,
        hmsFilters, "UTC")
      hmsPartitions.foreach { partition =>
        val hmsFiles = expUtil.hms_ext.listFiles(partition)
      }

      // list files, using DeltaLake
      val table = expUtil.delta.loadTable(Identifier.of(Array(deltaDBName), deltaTableName))
      val baseRelation = table.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(hmsFilters, Seq.empty)

    }

    for (i <- 0 until experimentIters) {
      // construct partition vals
      // the schema for HMS table and delta table should be the same!
      val partitionValsSeq = ArrayBuffer[List[Int]]()
      hmsTablejson.get("partitionSchema").forEach { partitionCol =>
        if (partitionCol.get("type").asText() == "INT") {
          val partitionMax = partitionCol.get("max").asInt()
          val rangeMin = dataGen.nextInt(1, partitionMax)
          val rangeMax = math.min(dataGen.nextInt(partitionRange(0), partitionRange(1))
            + rangeMin, partitionMax)
          partitionValsSeq.append(List(rangeMin, rangeMax))
        }
        else if (partitionCol.get("type").asText() == "DATE") {
          val minDate = LocalDate.parse(partitionCol.get("min").asText())
          val maxDate = LocalDate.parse(partitionCol.get("max").asText())
          val numDays = (ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt
          val rangeMin = dataGen.nextInt(1, numDays)
          val rangeMax = math.min(dataGen.nextInt(partitionRange(0), partitionRange(1))
            + rangeMin, numDays)
          partitionValsSeq.append(List(rangeMin, rangeMax))
        }
      }
      // the same filters can also be used for delta table
      val hmsFilters = constructHMSFilters(partitionValsSeq)
      val treeFilters = constructTreeFilters(partitionValsSeq)

      // list files, using TreeCatalog
      val treeStartTime = Instant.now()
      val treeFiles = expUtil.tree.listFilesByFilter(hmsDBName, hmsTableName, treeFilters, None)
      val treeEndTime = Instant.now()
      treeTime += Duration.between(treeStartTime, treeEndTime).toNanos()

      // list files, using HMS
      val hmsStartTime = Instant.now()
      val hmsPartitions = expUtil.hms.listPartitionsByFilter(hmsDBName, hmsTableName,
        hmsFilters, "UTC")
      hmsPartitions.foreach { partition =>
        val hmsFiles = expUtil.hms_ext.listFiles(partition)
      }
      val hmsEndTime = Instant.now()
      hmsTime += Duration.between(hmsStartTime, hmsEndTime).toNanos()

      // list files, using DeltaLake
      val deltaStartTime = Instant.now()
      val table = expUtil.delta.loadTable(Identifier.of(Array(deltaDBName), deltaTableName))
      val baseRelation = table.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(hmsFilters, Seq.empty)
      val deltaEndTime = Instant.now()
      deltaTime += Duration.between(deltaStartTime, deltaEndTime).toNanos()

    }

    printf("Tree listFiles Latency: " + treeTime / (1000000 * experimentIters) + "ms\n")
    printf("HMS listFiles Latency: " + hmsTime / (1000000 * experimentIters) + "ms\n")
    printf("DeltaLake listFiles Latency: " + deltaTime / (1000000 * experimentIters) + "ms\n")
  }

  private def constructHMSFilters(partitionVals : Seq[List[Int]]) : Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val partitionSchemaJson = hmsTablejson.get("partitionSchema")
    for (i <- 0 until partitionSchemaJson.size()) {
      val partitionCol = partitionSchemaJson.get(i)
      if (partitionCol.get("type").asText() == "INT") {
        val partitionName = partitionCol.get("name").asText()
        val minPartVal = partitionVals(i).min
        val maxPartVal = partitionVals(i).max
        val partitionPred = f"${partitionName} >= $minPartVal and " +
          f"${partitionName} <= $maxPartVal"
        filters.append(sqlParser.parseExpression(partitionPred))
      }
    }
    filters

  }

  private def constructTreeFilters(partitionVals : Seq[List[Int]]) : Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val partitionSchemaJson = hmsTablejson.get("partitionSchema")
    for (i <- 0 until partitionSchemaJson.size()) {
      val partitionCol = partitionSchemaJson.get(i)
      if (partitionCol.get("type").asText() == "INT") {
        val partitionName = partitionCol.get("name").asText()
        val minPartVal = "%012d".format(partitionVals(i).min)
        val maxPartVal = "%012d".format(partitionVals(i).max)
        val partitionPred = f"$partitionName >= '$partitionName=$minPartVal' and " +
          f"$partitionName <= '$partitionName=$maxPartVal'"
        filters.append(sqlParser.parseExpression(partitionPred))
      }
    }
    filters

  }

}

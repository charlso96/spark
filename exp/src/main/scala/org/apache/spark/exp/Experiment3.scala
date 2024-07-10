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
import java.lang.Runnable
import java.net.URI
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set
import scala.io.Source

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.math3.random.RandomDataGenerator

import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTableFile, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DateType, IntegralType, StringType}
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.tree.TreeTxn
import org.apache.spark.tree.grpc.Grpccatalog.TxnMode


/**
 * List of config parameters:
 * seed: seed for the random number generator, each thread adds its rank before seeding its own
 * summaryOutput: path of the output file to which result is appended
 * dryRunTime: Duration of dry run before the main experiment
 * experimentTime: Duration of the main experiment
 * numThreads: Number of threads that are executed
 * readWriteRatio: The ratio of read and write operations
 * dimSizeRange: range of number of dim tables to choose
 * factSizeRange: range of number of fact tables to choose
 * tableDistExponent: Exponent of the Zipfian distribution for choosing the tables
 * partitionDistExponent: Exponent of the Zipfian distribution for choosing partition values
 * partitionRange: Min and max of the partition range size to be scanned (from uniform distribution)
 * treeAddress: Address of the catalog service
 */

object Experiment3 {
  val expConfig = scala.collection.mutable.Map.empty[String, String]
  expConfig.put("seed", "0")
  expConfig.put("summaryOutput", "/tmp/summary.txt")
  expConfig.put("latencyOutput", "/tmp/latency.txt")
  expConfig.put("dryRunTime", "00:00:20")
  expConfig.put("experimentTime", "00:00:60")
  expConfig.put("numThreads", "16")
  expConfig.put("readWriteRatio", "50:50")
  expConfig.put("dimSizeRange", "1:7")
  expConfig.put("factSizeRange", "1:2")
  expConfig.put("tableDistExponent", "1")
  expConfig.put("partitionDistExponent", "1")
  expConfig.put("partitionRange", "1:30")
  expConfig.put("treeAddress", "localhost:9876")

  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.Experiment3 <dataConfig> <expConfig>\n")
      return
    }

    val jsonParser = new ObjectMapper
    // data configuration, including database name, table schema etc.
    val dataConfig = jsonParser.readTree(Source.fromFile(args(0)).mkString)
    // experiment configuration

    jsonParser.readTree(Source.fromFile(args(1)).mkString).fields().forEachRemaining { config =>
      expConfig.put(config.getKey, config.getValue.asText())
    }

    val seed = expConfig("seed").toInt
    val summaryOutput = expConfig("summaryOutput")
    val latencyOutput = expConfig("latencyOutput")
    val dryRunTime = convertToMilliseconds(expConfig("dryRunTime"))
    val experimentTime = convertToMilliseconds(expConfig("experimentTime"))
    val numThreads = expConfig("numThreads").toInt
    val threads = ArrayBuffer[Thread]()
    val rwRatio = expConfig("readWriteRatio")
    val dimSizeRange = expConfig("dimSizeRange")
    val factSizeRange = expConfig("factSizeRange")
    val tableDistExponent = expConfig("tableDistExponent")
    val partitionDistExponent = expConfig("partitionDistExponent")
    val partitionRange = expConfig("partitionRange")

    val execDryRun = new AtomicBoolean(false)
    val execExperiment = new AtomicBoolean(false)
    val totalNumCommits = new AtomicLong(0)
    val totalNumAborts = new AtomicLong(0)

    // array of arrays for collecting latencies of txns executed by all threads
    val totalCommitLatencies = ArrayBuffer[ArrayBuffer[Long]]()
    val totalAbortLatencies = ArrayBuffer[ArrayBuffer[Long]]()

    for (i <- 0 until numThreads) {
      val contExpConfig = expConfig.toMap
      val commitLatencies = ArrayBuffer[Long]()
      val abortLatencies = ArrayBuffer[Long]()
      totalCommitLatencies.append(commitLatencies)
      totalAbortLatencies.append(abortLatencies)
      threads.append(new Thread(new threadOps(dataConfig, contExpConfig, seed + i, execDryRun,
        execExperiment, totalNumCommits, totalNumAborts, commitLatencies, abortLatencies)))
    }

    // execute the dry run
    execDryRun.set(true)
    threads.foreach { thread =>
      thread.start()
    }
    Thread.sleep(dryRunTime)

    // execute the main experiment
    execExperiment.set(true)
    execDryRun.set(false)
    Thread.sleep(experimentTime)
    execExperiment.set(false)

    threads.foreach { thread =>
      thread.join()
    }

    val flatTotalCommitLatencies = totalCommitLatencies.flatten
    val flatTotalAbortLatencies = totalAbortLatencies.flatten
    val avgCommitLatency = if (flatTotalCommitLatencies.size > 0) {
      flatTotalCommitLatencies.sum / flatTotalCommitLatencies.size
    }
    else {
      0
    }

    // print the results
    val summaryWriter = new FileWriter(new File(summaryOutput), true)
    summaryWriter.write("{\"rwRatio\":\"" + rwRatio + "\", ")
    summaryWriter.write("\"numCommits\":" + totalNumCommits + ", ")
    summaryWriter.write("\"numAborts\":" + totalNumAborts + ", ")
    summaryWriter.write("\"throughput\":" +
      (totalNumCommits.get().toDouble * 1000) / experimentTime + ", ")
    summaryWriter.write("\"avgCommitLatency\":" + avgCommitLatency/1000000 + ", ")
    summaryWriter.write("\"dryRunTime\":" + dryRunTime/1000 + ", ")
    summaryWriter.write("\"experimentTime\":" + experimentTime/1000 + ", ")
    summaryWriter.write("\"numThreads\":" + numThreads + ", ")
    summaryWriter.write("\"dimSizeRange\":\"" + dimSizeRange + "\", ")
    summaryWriter.write("\"factSizeRange\":\"" + factSizeRange + "\", ")
    summaryWriter.write("\"tableDistExponent\":" + tableDistExponent + ", ")
    summaryWriter.write("\"partitionDistExponent\":" + partitionDistExponent + ", ")
    summaryWriter.write("\"partitionRange\":\"" + partitionRange + "\"}")
    summaryWriter.write("\n")
    summaryWriter.close()
//
    val latencyWriter = new FileWriter(new File(latencyOutput), true)
    latencyWriter.write(flatTotalCommitLatencies.mkString("[", ",", "]"))
    latencyWriter.write(flatTotalAbortLatencies.mkString("[", ",", "]"))
    latencyWriter.write("\n")
    latencyWriter.close()

  }

  private def convertToMilliseconds(time : String): Int = {
    val parts = time.split(":").map(_.toInt)
    val hours = parts(0)
    val minutes = parts(1)
    val seconds = parts(2)
    (hours * 3600 + minutes * 60 + seconds) * 1000
  }

  private class threadOps(dataConfig : JsonNode, expConfig : Map[String, String], seed : Long,
                          execDryRun : AtomicBoolean, execExperiment : AtomicBoolean,
                          totalNumCommits: AtomicLong, totalNumAborts: AtomicLong,
                          commitLatencies: ArrayBuffer[Long], abortLatencies: ArrayBuffer[Long])
      extends Runnable{
    private val rwRatio = expConfig("readWriteRatio").split(":").map(_.toInt)
    private val dimSizeRange = expConfig("dimSizeRange").split(":").map(_.toInt)
    private val factSizeRange = expConfig("factSizeRange").split(":").map(_.toInt)
    private val tableDistExponent = expConfig("tableDistExponent").toDouble
    private val partitionDistExponent = expConfig("partitionDistExponent").toDouble
    private val partitionRange = expConfig("partitionRange")
      .split(":").map(_.toInt)
    private val treeAddress = expConfig("treeAddress")

    private val dbName = dataConfig.get("databaseName").asText()
    private val tablesJson = dataConfig.get("tables")
    private val totalRatio = rwRatio.sum
    private var numCommits = 0
    private var numAborts = 0

    private val factTables = ArrayBuffer[JsonNode]()
    private val dimTables = ArrayBuffer[JsonNode]()
    tablesJson.forEach { tableJson =>
      if (tableJson.get("partitionSchema").isEmpty) {
        dimTables.append(tableJson)
      }
      else {
        factTables.append(tableJson)
      }
    }

    private val tree = new TreeExternalCatalog(treeAddress)
    private val dataGen = new RandomDataGenerator()
    dataGen.reSeed(seed)
    private val sqlParser = new SparkSqlParser()

    override def run() : Unit = {
      while (execDryRun.get()) {
        runCycle(true)
      }

      while (execExperiment.get()) {
        runCycle(false)
      }

      totalNumCommits.addAndGet(numCommits)
      totalNumAborts.addAndGet(numAborts)

    }

    // helper function for running a single cycle
    private def runCycle(isDryRun : Boolean): Unit = {
      val read = dataGen.nextInt(1, totalRatio) <= rwRatio(0)

      // pick dimension tables
      val dimSize = dataGen.nextInt(dimSizeRange(0), dimSizeRange(1))
      val dimTableIndices: Set[Int] = Set.empty[Int]
      while (dimTableIndices.size < dimSize) {
        // there is some skew in selection of dimension table
        dimTableIndices.add(dataGen.nextZipf(dimTables.size, tableDistExponent) - 1)
      }
      val dimTableJsons = ArrayBuffer[JsonNode]()
      dimTableIndices.foreach { tableIndex =>
        dimTableJsons.append(dimTables(tableIndex))
      }

      // choose facts table
      val factSize = dataGen.nextZipf(factSizeRange(0), factSizeRange(1))
      val factTableIndices: Set[Int] = Set.empty[Int]
      while (factTableIndices.size < factSize) {
        // there is some skew in selection of facts table
        factTableIndices.add(dataGen.nextZipf(factTables.size, tableDistExponent) - 1)
      }
      val factTableJsons = ArrayBuffer[JsonNode]()
      factTableIndices.foreach { tableIndex =>
        factTableJsons.append(factTables(tableIndex))
      }

      // randomly pick partition ranges to be read
      // use array of arrays as  table may have multilevel partitions
      val factTablePartitionRanks = ArrayBuffer[ArrayBuffer[List[Int]]]()
      factTableJsons.foreach { tableJson =>
        val partitionValsSeq = ArrayBuffer[List[Int]]()
        tableJson.get("partitionSchema").forEach { partitionCol =>
          if (partitionCol.get("type").asText() == "INT") {
            val partitionMax = partitionCol.get("max").asInt()
            val rangeMin = dataGen.nextZipf(partitionMax, partitionDistExponent)
            val rangeMax = math.min(dataGen.nextInt(partitionRange(0), partitionRange(1))
              + rangeMin, partitionMax)
            partitionValsSeq.append(List(rangeMin, rangeMax))
          }
          else if (partitionCol.get("type").asText() == "DATE") {
            val minDate = LocalDate.parse(partitionCol.get("min").asText())
            val maxDate = LocalDate.parse(partitionCol.get("max").asText())
            val numDays = (ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt
            val rangeMin = dataGen.nextZipf(numDays, partitionDistExponent)
            val rangeMax = math.min(dataGen.nextInt(partitionRange(0), partitionRange(1))
              + rangeMin, numDays)
            partitionValsSeq.append(List(rangeMin, rangeMax))
          }
        }
        factTablePartitionRanks.append(partitionValsSeq)
      }

      var retries = 0
      var success = false
      while (retries < 3 && !success) {
        // read operation
        if (read) {
          val startTime = Instant.now()
          val txn = tree.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
          scanTables(dimTableJsons, factTableJsons, factTablePartitionRanks, txn)
          if (tree.commit(txn.get)) {
            success = true
          }
          val endTime = Instant.now()
          if (!isDryRun) {
            if (success) {
              commitLatencies.append(Duration.between(startTime, endTime).toNanos())
            }
            else {
              abortLatencies.append(Duration.between(startTime, endTime).toNanos())
            }
          }
        }
        // write operation
        else {
          val startTime = Instant.now()
          val txn = tree.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
          addBatch(dimTableJsons, factTableJsons, factTablePartitionRanks, txn)
          if (tree.commit(txn.get)) {
            success = true
          }
          val endTime = Instant.now()
          if (!isDryRun) {
            if (success) {
              commitLatencies.append(Duration.between(startTime, endTime).toNanos())
            }
            else {
              abortLatencies.append(Duration.between(startTime, endTime).toNanos())
            }
          }
        }

        // if not dry run, collect the results
        if (!isDryRun) {
          if (success) {
            numCommits += 1

          }
          else {
            numAborts += 1
          }
        }

        retries += 1
      }
    }


    // joinTableJsons, joinTablePartitionVals, txn
    private def scanTables(dimTableJsons : Seq[JsonNode], factTableJsons : Seq[JsonNode],
                           factTablePartitionRanks: Seq[Seq[List[Int]]],
                           txn: Option[TreeTxn]) : Unit = {

      dimTableJsons.foreach { dimTableJson =>
        val dimTable = tree.getTable(dbName, dimTableJson.get("name").asText(), txn)
        if (dimTable == null) {
          if (txn.get.isOK()) {
            txn.get.setAbort(true)
            tree.commit(txn.get)
          }
          return
        }

        val fileList = tree.listFiles(dimTable, txn)
        if (!txn.get.isOK()) {
          return
        }
      }

      for (i <- factTableJsons.indices) {
        val tableJson = factTableJsons(i)
        val ranks = factTablePartitionRanks(i)
        val table = tree.getTable(dbName, tableJson.get("name").asText(), txn)
        if (table == null) {
          if (txn.get.isOK()) {
            txn.get.setAbort(true)
            tree.commit(txn.get)
          }
          return
        }

        val filters = ArrayBuffer[Expression]()
        val partitionSchema = table.partitionSchema
        val partitionSchemaJson = tableJson.get("partitionSchema")

        partitionSchema.indices.foreach{ i =>
          val partitionCol = partitionSchema(i)
          if (partitionCol.dataType.isInstanceOf[IntegralType]) {
            val partitionMax = partitionSchemaJson.get(i).get("max").asInt()
            val minPartVal = "%012d".format(partitionMax - ranks(i).max + 1)
            val maxPartVal = "%012d".format(partitionMax - ranks(i).min + 1)
            val partitionPred = f"${partitionCol.name} >= '${partitionCol.name}=$minPartVal' and " +
              f"${partitionCol.name} <= '${partitionCol.name}=$maxPartVal'"
            filters.append(sqlParser.parseExpression(partitionPred))
          }
          else {
            val maxDate = LocalDate.parse(partitionSchemaJson.get(i).get("max").asText())
            val minPartVal = maxDate.minusDays(ranks(i).max - 1).toString
            val maxPartVal = maxDate.minusDays(ranks(i).min - 1).toString
            val partitionPred = f"${partitionCol.name} >= '${partitionCol.name}=$minPartVal' and " +
              f"${partitionCol.name} <= '${partitionCol.name}=$maxPartVal'"
            filters.append(sqlParser.parseExpression(partitionPred))
          }
        }

        val fileList = tree.listFilesByFilter(table, filters, txn)
        if (!txn.get.isOK()) {
          return
        }
      }

    }

    private def addBatch(dimTableJsons : Seq[JsonNode], factTableJsons : Seq[JsonNode],
                         factTablePartitionRanks: Seq[Seq[List[Int]]],
                         txn: Option[TreeTxn]) : Unit = {

      if (!dimTableJsons.isEmpty) {
        // scan dimension tables to join with
        scanTables(dimTableJsons, Seq.empty[JsonNode], factTablePartitionRanks, txn)
        if (!txn.get.isOK()) {
          return
        }
      }
      // The first fact table is the destination
      val destTableJson = factTableJsons(0)
      val destTablePartitionRanks = factTablePartitionRanks(0)
      val destTable = tree.getTable(dbName, destTableJson.get("name").asText(), txn)
      if (destTable == null) {
        if (txn.get.isOK()) {
          txn.get.setAbort(true)
          tree.commit(txn.get)
        }
        return
      }
      // randomly pick the destination partition, using Zipf distribution
      val destPartSpec = scala.collection.mutable.Map[String, String]()
      val partitionSchema = destTable.partitionSchema
      val partitionSchemaJson = destTableJson.get("partitionSchema")

      partitionSchema.indices.foreach { i =>
        val partitionCol = destTable.partitionSchema(i)
        // if integer type, partition with maximum value is most likely
        if (partitionCol.dataType.isInstanceOf[IntegralType]) {
          val partitionMax = partitionSchemaJson.get(i).get("max").asInt()
          val rank = destTablePartitionRanks(i)
          destPartSpec.put(partitionCol.name, (partitionMax - rank.min + 1).toString)
        }
        // if date type, partition with most recent date is most likely
        if (partitionCol.dataType == DateType) {
          val maxDate = LocalDate.parse(partitionSchemaJson.get(i).get("max").asText())
          val rank = destTablePartitionRanks(i)
          destPartSpec.put(partitionCol.name, maxDate.minusDays(rank.min - 1).toString)
        }

      }

      val immutableDestPartSpec = destPartSpec.toMap

      val files = ArrayBuffer[CatalogTableFile]()
      // only add 1 file since we do not want the number of files per partition to blow up
      for (i <- 0 until 1) {
        val filePath = destTable.location.getPath + "/" + UUID.randomUUID()
        val storage = CatalogStorageFormat(Some(new URI(filePath)), destTable.storage.inputFormat,
          destTable.storage.outputFormat, destTable.storage.serde, false, destTable.properties)
        val stats = genRandomFileStats(destTable)
        files.append(CatalogTableFile(storage, immutableDestPartSpec, 100, stats = Some(stats)))
      }

      // if table is partitioned, check if the destination partition exists
      if (!destTable.partitionColumnNames.isEmpty) {
        val partition = tree.getPartition(destTable, immutableDestPartSpec, txn)
        if (!txn.get.isOK) {
          return
        }
        if (partition.isEmpty) {
          val newPartition = CatalogTablePartition(immutableDestPartSpec, destTable.storage)
          tree.createPartition(destTable, newPartition, txn)
        }
      }
      // finally add batch of files
      tree.addFiles(destTable, files, txn)

    }

    private def genRandomFileStats(table : CatalogTable): CatalogStatistics = {
      val schema = table.schema
      val sizeInBytes = BigInt(100)
      val rowCount = Some(BigInt(1))

      val colStats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
      for (i <- 0 until schema.size - table.partitionColumnNames.size) {
        if (schema(i).dataType.isInstanceOf[IntegralType]) {
          val randVal = Some(dataGen.nextInt(0, 1000000).toString)
          val colStat = CatalogColumnStat(None, randVal, randVal, Some(BigInt(0)),
              None, None, None, 1)
          colStats.put(schema(i).name, colStat)
        }
        else if (schema(i).dataType == StringType) {
          val randVal = Some(genRandomWord(3, 10))
          val colStat = CatalogColumnStat(None, randVal, randVal, Some(BigInt(0)),
            None, None, None, 1)
          colStats.put(schema(i).name, colStat)
        }
      }

      CatalogStatistics(sizeInBytes, rowCount, colStats.toMap)
    }

    private def genRandomWord(minLength: Int, maxLength: Int): String = {
      val length = dataGen.nextInt(minLength, maxLength)
      val sb = new StringBuilder
      for (_ <- 1 to length) {
        val randomChar = dataGen.nextInt(0, 62) match {
          case i if i < 26 => (i + 'a').toChar // a-z
          case i if i < 52 => (i - 26 + 'A').toChar // A-Z
          case i => (i - 52 + '0').toChar // 0-9
        }
        sb.append(randomChar)
      }
      sb.toString()
    }

  }

}

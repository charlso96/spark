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

// import java.io.File
// import java.io.FileWriter
import java.lang.Runnable
import java.net.URI
// import java.time.Duration
// import java.time.Instant
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
 * dryRunTime: Time for dry run before the main experiment
 * experimentTime: Time for the main experiment
 * numThreads: Number of threads that are executed
 * tableNumExponent: Exponent of Zipfian distribution for getting the number of tables
 * tableDistExpoenent: Exponent of the Zipfian distribution for choosing the tables
 * partitionDistExponent: Expoenent of the Zipfian distribution for choosing partition values
 *
 */

object Experiment3 {
  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.Experiment3 <dataConfig> <expConfig>\n")
      return
    }

    val jsonParser = new ObjectMapper
    // data configuration, including database name, table schema etc.
    val dataConfig = jsonParser.readTree(Source.fromFile(args(0)).mkString)
    // experiment configuration
    val expConfig = scala.collection.mutable.Map.empty[String, String]
    jsonParser.readTree(Source.fromFile(args(1)).mkString).fields().forEachRemaining { config =>
      expConfig.put(config.getKey, config.getValue.asText())
    }

    val dryRunTime = convertToMilliseconds(expConfig.getOrElse("dryRunTime", "00:00:30"))
    val experimentTime = convertToMilliseconds(expConfig.getOrElse("experimentTime", "00:00:30"))
    val numThreads = expConfig.getOrElse("numThreads", "1").toInt
    val threads = ArrayBuffer[Thread]()

    val execDryRun = new AtomicBoolean(false)
    val execExperiment = new AtomicBoolean(false)
    val numCommits = new AtomicLong(0)
    val numAborts = new AtomicLong(0)

    for (i <- 0 until numThreads) {
      threads.append(new Thread(new threadOps(dataConfig, expConfig.toMap, i, execDryRun,
        execExperiment, numCommits, numAborts)))
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

    // print the results
    printf("Number of Commits: " + numCommits.toString + "\n")
    printf("Number of Aborts: " + numAborts.toString + "\n")
    printf("Throughput: " + (numCommits.get().toDouble * 1000) / experimentTime + "\n")

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
                          totalNumCommits: AtomicLong, totalNumAborts: AtomicLong) extends Runnable{

    private val tree = new TreeExternalCatalog()
    private val dbName = dataConfig.get("databaseName").asText()
    private val dataGen = new RandomDataGenerator()
    private val sqlParser = new SparkSqlParser()
    dataGen.reSeed(seed)
    private val rwRatio = expConfig.getOrElse("readWriteRatio", "50:50").split(":").map(_.toInt)
    private val tableNumExponent = expConfig.getOrElse("tableNumExponent", "1").toDouble
    private val tableDistExponent = expConfig.getOrElse("tableDistExponent", "1").toDouble
    private val partitionDistExponent = expConfig.getOrElse("partitionDistExponent", "1").toDouble
    private val tablesJson = dataConfig.get("tables")
    private val numTables = tablesJson.size()
    private val totalRatio = rwRatio.sum
    private var numCommits = 0
    private var numAborts = 0

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

      // pick tables to join
      // do not pick all tables as 1 of them is destination table
      val joinSize = math.min(dataGen.nextZipf(numTables, tableNumExponent), numTables - 1)
      val joinTableIndices: Set[Int] = Set.empty[Int]
      while (joinTableIndices.size < joinSize) {
        joinTableIndices.add(dataGen.nextZipf(numTables, tableDistExponent) - 1)
      }
      val joinTableJsons = ArrayBuffer[JsonNode]()
      joinTableIndices.foreach { tableIndex =>
        joinTableJsons.append(tablesJson.get(tableIndex))
      }
      // pick destination table
      var destTableIndex = numTables - dataGen.nextZipf(numTables, tableDistExponent)
      while (joinTableIndices.contains(destTableIndex)) {
        destTableIndex = numTables - dataGen.nextZipf(numTables, tableDistExponent)
      }
      val destTableJson = tablesJson.get(destTableIndex)

      // randomly pick partition ranges to be read
      // use array of arrays as  table may have multilevel partitions
      val joinTablePartitionRanks = ArrayBuffer[ArrayBuffer[List[Int]]]()
      joinTableJsons.foreach { tableJson =>
        val partitionValsSeq = ArrayBuffer[List[Int]]()
        tableJson.get("partitionSchema").forEach { partitionCol =>
          if (partitionCol.get("type").asText() == "INT") {
            val partitionMax = partitionCol.get("max").asInt()
            partitionValsSeq.append(List(dataGen.nextZipf(partitionMax, partitionDistExponent),
              dataGen.nextZipf(partitionMax, partitionDistExponent)))
          }
          else if (partitionCol.get("type").asText() == "DATE") {
            val minDate = LocalDate.parse(partitionCol.get("min").asText())
            val maxDate = LocalDate.parse(partitionCol.get("max").asText())
            val numDays = (ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt
            partitionValsSeq.append(List(dataGen.nextZipf(numDays, partitionDistExponent),
              dataGen.nextZipf(numDays, partitionDistExponent)))
          }
        }
        joinTablePartitionRanks.append(partitionValsSeq)
      }

      // randomly pick destination partition of addFiles operation
      val destTablePartitionRanks = ArrayBuffer[Int]()
      destTableJson.get("partitionSchema").forEach { partitionCol =>
        if (partitionCol.get("type").asText() == "INT") {
          val partitionMax = partitionCol.get("max").asInt()
          destTablePartitionRanks.append(dataGen.nextZipf(partitionMax, partitionDistExponent))
        }
        else if (partitionCol.get("type").asText() == "DATE") {
          val minDate = LocalDate.parse(partitionCol.get("min").asText())
          val maxDate = LocalDate.parse(partitionCol.get("max").asText())
          val numDays = (ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt
          destTablePartitionRanks.append(dataGen.nextZipf(numDays, partitionDistExponent))
        }
      }

      var retries = 0
      var success = false
      while (retries < 3 && !success) {
        // read operation
        if (read) {
          val txn = tree.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
          scanTables(joinTableJsons, joinTablePartitionRanks, txn)
          if (tree.commit(txn.get)) {
            success = true
          }

        }
        // write operation
        else {
          val txn = tree.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
          addBatch(destTableJson, destTablePartitionRanks, joinTableJsons,
            joinTablePartitionRanks, txn)
          if (tree.commit(txn.get)) {
            success = true
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
    private def scanTables(joinTableJsons : Seq[JsonNode],
                           joinTablePartitionRanks: Seq[Seq[List[Int]]],
                           txn: Option[TreeTxn]) : Unit = {
      for (i <- 0 until joinTableJsons.size) {
        val tableJson = joinTableJsons(i)
        val ranks = joinTablePartitionRanks(i)
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

        partitionSchema.indices.foreach{ i =>
          val partitionCol = partitionSchema(i)
          if (partitionCol.dataType.isInstanceOf[IntegralType]) {
            val partitionMax = tableJson.get("partitionMax").asInt()
            val minPartVal = "%012d".format(partitionMax - ranks(i).max + 1)
            val maxPartVal = "%012d".format(partitionMax - ranks(i).min + 1)
            val partitionPred = f"${partitionCol.name} >= '${partitionCol.name}=$minPartVal' and " +
              f"${partitionCol.name} <= 'partitionCol.name=$maxPartVal'"
            filters.append(sqlParser.parseExpression(partitionPred))
          }
          else {
            val maxDate = LocalDate.parse(tableJson.get("partitionMax").asText())
            val minPartVal = maxDate.minusDays(ranks(i).max - 1).toString
            val maxPartVal = maxDate.minusDays(ranks(i).min - 1).toString
            val partitionPred = f"${partitionCol.name} >= '${partitionCol.name}=$minPartVal' and " +
              f"${partitionCol.name} <= 'partitionCol.name=$maxPartVal'"
            filters.append(sqlParser.parseExpression(partitionPred))
          }
        }

        val fileList = tree.listFilesByFilter(table, filters, txn)
        if (!txn.get.isOK()) {
          return
        }
      }

    }

    private def addBatch(destTableJson : JsonNode, destTablePartitionRanks: Seq[Int],
                         joinTableJsons: Seq[JsonNode],
                         joinTablePartitionRanks: Seq[Seq[List[Int]]],
                         txn: Option[TreeTxn]) : Unit = {

      if (!joinTableJsons.isEmpty) {
        // pick partition range for each table and scan each table
        scanTables(joinTableJsons, joinTablePartitionRanks, txn)
        if (!txn.get.isOK()) {
          return
        }
      }
      // get the destination table to which files are added
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
      partitionSchema.indices.foreach { i =>
        val partitionCol = destTable.partitionSchema(i)
        // if integer type, partition with maximum value is most likely
        if (partitionCol.dataType.isInstanceOf[IntegralType]) {
          val partitionMax = destTableJson.get("partitionMax").asInt()
          val rank = destTablePartitionRanks(i)
          destPartSpec.put(partitionCol.name, (partitionMax - rank + 1).toString)
        }
        // if date type, partition with most recent date is most likely
        if (partitionCol.dataType == DateType) {
          val maxDate = LocalDate.parse(destTableJson.get("partitionMax").asText())
          val rank = destTablePartitionRanks(i)
          destPartSpec.put(partitionCol.name, maxDate.minusDays(rank - 1).toString)
        }

      }

      val immutableDestPartSpec = destPartSpec.toMap

      val files = ArrayBuffer[CatalogTableFile]()
      // add a batch of 10 files
      for (i <- 0 until 10) {
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

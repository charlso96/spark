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
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set
import scala.io.Source

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.math3.random.RandomDataGenerator

import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTableFile, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DateType, IntegralType}
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.tree.TreeTxn
import org.apache.spark.tree.grpc.Grpccatalog.TxnMode


object Experiment3 {
  def main(args: Array[String]): Unit = {
    if (args.size != 4) {
      print("Usage: spark-class org.apache.spark.exp.Experiment3 <dataConfig> " +
        "<numThreads> <iters> <rwRatio>\n")
      return
    }

    val jsonParser = new ObjectMapper
    val dataConfig = jsonParser.readTree(Source.fromFile(args(0)).mkString)
    val dbName = dataConfig.get("databaseName").asText()

    val numThreads = args(1).toInt
    val iters = args(2).toInt
    val rwRatio = args(3).split(":").toSeq.map(_.toInt)

    val threads = ArrayBuffer[Thread]()
    val numAborts = new AtomicLong(0)
    for (i <- 0 until numThreads) {
      threads.append(new Thread(new threadOps(new TreeExternalCatalog(), dbName, dataConfig, new
        RandomDataGenerator(), new SparkSqlParser(), rwRatio, iters, numAborts)))
    }

    val startTime = Instant.now()
    threads.foreach { thread =>
      thread.start()
    }

    threads.foreach { thread =>
      thread.join()
    }
    val endTime = Instant.now()

    printf("Number of Aborts:" + numAborts.toString + "\n")
    printf("Duration: " + Duration.between(startTime, endTime).toMillis().toString + " ms\n")

  }

  private class threadOps(tree : TreeExternalCatalog, db: String, dataConfig : JsonNode,
                          dataGen : RandomDataGenerator, sqlParser :
                          SparkSqlParser, rwRatio : Seq[Int], iters : Int,
                          totalNumAborts : AtomicLong) extends Runnable {
    override def run() : Unit = {
      val tablesJson = dataConfig.get("tables")
      val numTables = tablesJson.size()
      val totalRatio = rwRatio.sum
      var numAborts = 0

      for (i <- 0 until iters) {
        val joinSize = dataGen.nextZipf(numTables, 1)
        val joinTableIndices: Set[Int] = Set.empty[Int]
        while (joinTableIndices.size < joinSize) {
          joinTableIndices.add(dataGen.nextZipf(numTables, 1) - 1)
        }
        val joinTableJsons = ArrayBuffer[JsonNode]()
        joinTableIndices.foreach{ tableIndex =>
          joinTableJsons.append(tablesJson.get(tableIndex))
        }

        // read operation
        if (dataGen.nextInt(1, totalRatio) <= rwRatio(0)) {
          val txn = tree.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
          scanTables(joinTableJsons, txn)
          if (!tree.commit(txn.get)) {
            numAborts += 1
          }

        }
        // write operation
        else {
          val txn = tree.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
          val destTableJson = joinTableJsons(joinTableJsons.size - 1)
          joinTableJsons.remove(joinTableJsons.size - 1)
          addBatch(destTableJson, joinTableJsons, txn)
          if (!tree.commit(txn.get)) {
            numAborts += 1
          }
        }

      }

      totalNumAborts.addAndGet(numAborts)

    }

    private def scanTables(joinTableJsons : Seq[JsonNode], txn: Option[TreeTxn]) : Unit = {
      joinTableJsons.foreach { tableJson =>
        val table = tree.getTable(db, tableJson.get("name").asText(), txn)
        if (table == null) {
          if (txn.get.isOK()) {
            txn.get.setAbort(true)
            tree.commit(txn.get)
          }
          return
        }

        val filters = ArrayBuffer[Expression]()
        // assume single level partition for now
        table.partitionSchema.foreach { partitionCol =>
          if (partitionCol.dataType.isInstanceOf[IntegralType]) {
            val partitionMax = tableJson.get("partitionMax").asInt()
            val ranks = List(dataGen.nextZipf(partitionMax, 1), dataGen.nextZipf(partitionMax, 1))
            val minPartVal = "%012d".format(partitionMax - ranks.max + 1)
            val maxPartVal = "%012d".format(partitionMax - ranks.min + 1)
            val partitionPred = f"${partitionCol.name} >= '${partitionCol.name}=$minPartVal' and " +
              f"${partitionCol.name} <= 'partitionCol.name=$maxPartVal'"
            filters.append(sqlParser.parseExpression(partitionPred))
          }
          if (partitionCol.dataType == DateType) {
            val minDate = LocalDate.parse(tableJson.get("partitionMin").asText())
            val maxDate = LocalDate.parse(tableJson.get("partitionMax").asText())
            val numDays = (ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt
            val ranks = List(dataGen.nextZipf(numDays, 1), dataGen.nextZipf(numDays, 1))
            val minPartVal = maxDate.minusDays(ranks.max - 1).toString
            val maxPartVal = maxDate.minusDays(ranks.min - 1).toString
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

    private def addBatch(destTableJson : JsonNode, joinTableJsons : Seq[JsonNode],
                         txn: Option[TreeTxn]) : Unit = {

      if (!joinTableJsons.isEmpty) {
        // pick partition range for each table and scan each table
        scanTables(joinTableJsons, txn)
        if (!txn.get.isOK()) {
          return
        }
      }
      // get the destination table to which files are added
      val destTable = tree.getTable(db, destTableJson.get("name").asText(), txn)
      if (destTable == null) {
        if (txn.get.isOK()) {
          txn.get.setAbort(true)
          tree.commit(txn.get)
        }
        return
      }
      // randomly pick the destination partition, using Zipf distribution
      val destPartSpec = scala.collection.mutable.Map[String, String]()
      destTable.partitionSchema.foreach { partitionCol =>
        // if integer type, partition with maximum value is most likely
        if (partitionCol.dataType.isInstanceOf[IntegralType]) {
          val partitionMax = destTableJson.get("partitionMax").asInt()
          val rank = dataGen.nextZipf(partitionMax, 1)
          destPartSpec.put(partitionCol.name, (partitionMax - rank + 1).toString)
        }
        // if date type, partition with most recent date is most likely
        if (partitionCol.dataType == DateType) {
          val minDate = LocalDate.parse(destTableJson.get("partitionMin").asText())
          val maxDate = LocalDate.parse(destTableJson.get("partitionMax").asText())
          val rank = dataGen.nextZipf((ChronoUnit.DAYS.between(minDate, maxDate) + 1).toInt, 1)
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
        files.append(CatalogTableFile(storage, immutableDestPartSpec, 100))
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
  }

}

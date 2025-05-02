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

import java.io.{File, FileWriter}
import java.net.URI
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTableFile}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.tree.grpc.Grpccatalog.LockMode
import org.apache.spark.tree.grpc.Grpccatalog.TxnMode

object BreadthDepthExperiment {
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("summaryOutput", "/tmp/breadth-summary.json")
  misc_config.put("opOutput", "/tmp/breadth-op.json")
  misc_config.put("dryRunTime", "00:00:30")
  misc_config.put("experimentTime", "00:05:00")
  misc_config.put("numThreads", "10")
  misc_config.put("totalNumThreads", "30")
  misc_config.put("version", "1")
  // Ratio is read:write operations in order
  misc_config.put("workloadRatio", "50:50")
  misc_config.put("selectivity", "0.05")
  misc_config.put("fanOut", "2")
  misc_config.put("insertRatio", "0.0001")
  misc_config.put("scaleFactor", "100T")
  misc_config.put("treeAddress", "localhost:9876")


  def main(args: Array[String]): Unit = {
    if (args.size != 1) {
      print("Usage: spark-class org.apache.spark.exp.BreadthDepthExperiment " +
        "<config>\n")
      return
    }

    val json_parser = new ObjectMapper
    // read in data config
    val data_config_json = json_parser.readTree(Source.fromFile(args(0)).mkString)
    val database_name = data_config_json.get("databaseName").asText()
    data_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val table_config = new TableConfig(data_config_json.get("tables").get(0),
      misc_config("scaleFactor"))
    val op_generator = new OpGenerator(misc_config("workloadRatio"))
    val exec_dry_run = new AtomicBoolean(false)
    val exec_experiment = new AtomicBoolean(false)
    val total_num_commits = new AtomicLong(0)
    val total_num_aborts = new AtomicLong(0)
    val dry_run_time = convertToMilliseconds(misc_config("dryRunTime"))
    val experiment_time = convertToMilliseconds(misc_config("experimentTime"))
    val total_op_data = ArrayBuffer[ArrayBuffer[OpData]]()
    val threads = ArrayBuffer[Thread]()

    for (i <- 0 until misc_config("numThreads").toInt) {
      val op_data_array = ArrayBuffer[OpData]()
      total_op_data.append(op_data_array)
      threads.append(new Thread(new threadOps(database_name, misc_config.toMap, table_config,
        op_generator, exec_dry_run, exec_experiment, total_num_commits, total_num_aborts,
        op_data_array)))
    }

    // execute the dry run
    exec_dry_run.set(true)
    threads.foreach { thread =>
      thread.start()
    }
    Thread.sleep(dry_run_time)

    // execute the main experiment
    exec_experiment.set(true)
    exec_dry_run.set(false)
    Thread.sleep(experiment_time)
    exec_experiment.set(false)

    threads.foreach { thread =>
      thread.join()
    }

    // print the results
    val summaryWriter = new FileWriter(new File(misc_config("summaryOutput")), true)
    summaryWriter.write("{\"workloadRatio\":\"" + misc_config("workloadRatio") + "\", ")
    summaryWriter.write("\"totalNumThreads\":" + misc_config("totalNumThreads") + ", ")
    summaryWriter.write("\"version\":" + misc_config("version") + ", ")
    summaryWriter.write("\"numCommits\":" + total_num_commits + ", ")
    summaryWriter.write("\"numAborts\":" + total_num_aborts + ", ")
    summaryWriter.write("\"throughput\":" +
      (total_num_commits.get().toDouble * 1000) / experiment_time + ", ")
    summaryWriter.write("\"dryRunTime\":" + dry_run_time / 1000 + ", ")
    summaryWriter.write("\"experimentTime\":" + experiment_time / 1000 + ", ")
    summaryWriter.write("\"fanOut\":" + misc_config("fanOut") + ", ")
    summaryWriter.write("\"height\":" + (table_config.partition_schema.length + 1) + ", ")
    summaryWriter.write("\"numThreads\":" + misc_config("numThreads") + "}")
    summaryWriter.write("\n")
    summaryWriter.flush()
    summaryWriter.close()

    val opWriter = new FileWriter(new File(misc_config("opOutput")), true)
    total_op_data.flatten.foreach { op_data : OpData =>
      opWriter.write("{\"workloadRatio\":\"" + misc_config("workloadRatio") + "\", ")
      opWriter.write("\"totalNumThreads\":" + misc_config("totalNumThreads") + ", ")
      opWriter.write("\"version\":" + misc_config("version") + ", ")
      opWriter.write("\"latency\":" + op_data.latency + ", ")
      opWriter.write("\"dataSent\":" + op_data.data_sent + ", ")
      opWriter.write("\"dataReceived\":" + op_data.data_received + ", ")
      opWriter.write("\"opType\":" + op_data.op_type + ", ")
      opWriter.write("\"misc\":\"" + op_data.misc + "\", ")
      opWriter.write("\"committed\":" + op_data.committed + "}")
      opWriter.write("\n")
    }
    opWriter.flush()
    opWriter.close()

  }

  private def convertToMilliseconds(time : String): Int = {
    val parts = time.split(":").map(_.toInt)
    val hours = parts(0)
    val minutes = parts(1)
    val seconds = parts(2)
    (hours * 3600 + minutes * 60 + seconds) * 1000
  }

  private class threadOps(database_name : String, misc_config: Map[String, String],
                          table_config: TableConfig, op_generator: OpGenerator,
                          exec_dry_run: AtomicBoolean, exec_experiment: AtomicBoolean,
                          total_num_commits: AtomicLong, total_num_aborts: AtomicLong,
                          op_data_array : ArrayBuffer[OpData])
    extends Runnable {

    private val tree_address = misc_config("treeAddress")

    private val tree_cat = new TreeExternalCatalog(tree_address)

    private val sqlParser = new SparkSqlParser()

    private val workload_generator = new WorkloadGenerator()

    private var num_commits = 0
    private var num_aborts = 0

    // a combined workload generator
    private class WorkloadGenerator() {
      private val insert_ratio = misc_config("insertRatio").toDouble
      private val num_files : Int = misc_config("numFiles").toInt
      private val partition_cardinalities : Array[Int] = table_config.partition_schema
        .map{ attr_config =>
        attr_config.cardinality.getOrElse(misc_config("fanOut").toLong).toInt
      }
      private val num_partitions = partition_cardinalities.product
      // num_partitions prob won't divide num_files evenly
      private val files_per_partition : Int = num_files / num_partitions
      private val threshold : Int = (num_files % num_partitions) * (files_per_partition + 1)
      private val selective_range : Int = (misc_config("selectivity").toDouble * num_files).toInt

      // first_part file index is divided by partition with an extra file due to remainder
      private def toPartIdx(file_idx : Int) : Int = {
        val first_part = math.min(threshold, file_idx)
        (first_part / (files_per_partition + 1)) + ((file_idx - first_part) / files_per_partition)
      }

      // Generate min and max file idx
      // Generate min and max partition prefix on partition columns and predicate filter on
      // the first unpartitioned column, which is clustered
      // Turn the filters to array of expressions and return
      def genRangeFilter() : ArrayBuffer[Expression] = {
        val min_file_idx = ThreadLocalRandom.current().nextInt(num_files - selective_range)
        val max_file_idx = min_file_idx + selective_range
        val min_partition_idx = toPartIdx(min_file_idx)
        val max_partition_idx = toPartIdx(max_file_idx)
        val min_partition_prefix = computePrefix(min_partition_idx)
        val max_partition_prefix = computePrefix(max_partition_idx)

        val filters = ArrayBuffer[Expression]()
        for (i <- table_config.partition_schema.indices) {
          val partition_col_name = table_config.partition_schema(i).name
          val min_idx_str = "%012d".format(min_partition_prefix(i))
          val max_idx_str = "%012d".format(max_partition_prefix(i))
          val min_part_pred = f"$partition_col_name >= '$partition_col_name=$min_idx_str'"
          val max_part_pred = f"$partition_col_name <= '$partition_col_name=$max_idx_str'"
          filters.append(sqlParser.parseExpression(min_part_pred))
          filters.append(sqlParser.parseExpression(max_part_pred))
        }

        // for more precise selectivity, add predicate filter on the first non-partitioned attribute
        val first_attr = table_config.schema(0)
        if (first_attr.clustered && first_attr.cardinality.isDefined) {
          val attr_cardinality = first_attr.cardinality.get
          val min_val = min_file_idx * attr_cardinality / num_files
          val max_val = (max_file_idx + 1) * attr_cardinality / num_files - 1
          val min_pred = f"${first_attr.name} >= $min_val"
          val max_pred = f"${first_attr.name} <= $max_val"
          filters.append(sqlParser.parseExpression(min_pred))
          filters.append(sqlParser.parseExpression(max_pred))
        }

        filters
      }

      def genInsertFiles(table : CatalogTable) : ArrayBuffer[CatalogTableFile] = {
        val file_idx = ThreadLocalRandom.current().nextInt(num_files)
        val partition_idx = toPartIdx(file_idx)
        val partition_prefix = computePrefix(partition_idx)

        // column statistics
        val col_stats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
        table_config.schema.foreach { attr_config =>
          // for this experiment, we assume that clustered attribute is an integer
          if (attr_config.clustered && attr_config.cardinality.isDefined) {
            val attr_cardinality = attr_config.cardinality.get
            val min_val = file_idx * attr_cardinality / num_files
            val max_val = (file_idx + 1) * attr_cardinality / num_files - 1

            val col_stat = CatalogColumnStat(None, Some(min_val.toString), Some(max_val.toString),
              Some(BigInt(0)), None, None, None, 1)
            col_stats.put(attr_config.name, col_stat)
          }
          else {
            var min = ""
            var max = ""
            // generate random min and max string/int
            attr_config.data_type match {
              case "VARCHAR" =>
                min = genRandomVarChar(attr_config.cardinality.get)
                val temp = genRandomVarChar(attr_config.cardinality.get)
                if (temp < min) {
                  max = min
                  min = temp
                }
                else {
                  max = temp
                }
              case "DECIMAL" =>
                val min_decimal = genRandomDecimal(attr_config.cardinality.get)
                val temp_decimal = genRandomDecimal(attr_config.cardinality.get)
                if (temp_decimal < min_decimal) {
                  max = min_decimal.toString
                  min = temp_decimal.toString
                }
                else {
                  min = min_decimal.toString
                  max = temp_decimal.toString
                }
              case "INT" =>
                min = ThreadLocalRandom.current().nextInt(attr_config.cardinality.get.
                  toInt).toString
                val temp = ThreadLocalRandom.current().nextInt(attr_config.cardinality.get.toInt)
                  .toString
                if (temp < min) {
                  max = min
                  min = temp
                }
                else {
                  max = temp
                }
            }
            val col_stat = CatalogColumnStat(None, Some(min), Some(max),
              Some(BigInt(0)), None, None, None, 1)
            col_stats.put(attr_config.name, col_stat)

          }
        }

        // Now, insert to fact table a single file with appropriate number of rows, file stats etc.
        val row_count = (insert_ratio * table_config.num_rows).toInt
        val size_in_bytes = row_count * table_config.bytes_per_row
        val file_stats = CatalogStatistics(size_in_bytes, Some(BigInt(row_count)), col_stats.toMap)

        val dest_part_spec = scala.collection.mutable.Map[String, String]()
        for (i <- table_config.partition_schema.indices) {
          dest_part_spec.put(table_config.partition_schema(i).name, partition_prefix(i).toString)
        }
        val immutable_dest_part_spec = dest_part_spec.toMap

        val files = new ArrayBuffer[CatalogTableFile]()
        val file_path = table.location.getPath + "/" + UUID.randomUUID()
        val storage = CatalogStorageFormat(Some(new URI(file_path)),
          table.storage.inputFormat, table.storage.outputFormat,
          table.storage.serde, false, table.properties)

        files.append(CatalogTableFile(storage, immutable_dest_part_spec,
          file_stats.sizeInBytes.toLong, stats = Some(file_stats)))

        files
      }

      private def computePrefix(partition_idx : Int) : ArrayBuffer[Int] = {
        val prefix = new ArrayBuffer[Int]()
        var quotient = partition_idx
        val iter = partition_cardinalities.reverseIterator
        while (iter.hasNext) {
          val cardinality = iter.next()
          prefix += (quotient % cardinality)
          quotient /= cardinality
        }
        prefix.reverse
      }

      private def genRandomDecimal(num_digits : Long) : Int = {
        ThreadLocalRandom.current().nextInt(math.pow(10, num_digits - 4).toInt)
      }

      private def genRandomVarChar(max_length : Long) : String = {
        val length = ThreadLocalRandom.current().nextInt(max_length.toInt)
        val char_buf = ArrayBuffer.fill(length)('A')
        for (i <- 0 until length) {
          char_buf(i) = (ThreadLocalRandom.current().nextInt(26) + 65).toChar
        }
        char_buf.mkString
      }
    }

    override def run(): Unit = {
      while (exec_dry_run.get()) {
        runCycle(None)
      }

      while (exec_experiment.get()) {
        val op_data = new OpData
        op_data_array.append(op_data)
        runCycle(Some(op_data))
      }

      total_num_commits.addAndGet(num_commits)
      total_num_aborts.addAndGet(num_aborts)

    }

    // helper function for running a single cycle
    private def runCycle(op_data : Option[OpData]): Unit = {
      val op = op_generator.genOp()

      val startTime = Instant.now()
      val success = op match {
        case 0 => writeOp(op_data)
        case 1 => readOp(op_data)
      }
      val endTime = Instant.now()

      // if not dry run, collect the results
      if (op_data.isDefined) {
        op_data.get.committed = success
        op_data.get.op_type = op
        op_data.get.latency = Duration.between(startTime, endTime).toNanos()
        if (success) {
          num_commits += 1
        }
        else {
          num_aborts += 1
        }
      }
    }

    private def writeOp(op_data : Option[OpData]): Boolean = {
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      val table_name = table_config.name

      // get the target fact table in IX mode or X mode, depending on partition level
      val table =
        if (table_config.partition_schema.isEmpty) {
          tree_cat.getTable(database_name, table_name, txn, Some(LockMode.LOCK_MODE_X))
        }
        else {
          tree_cat.getTable(database_name, table_name, txn, Some(LockMode.LOCK_MODE_IX))
        }
      // scan a range of files first
      val filters = workload_generator.genRangeFilter()
      tree_cat.listFilesByFilter(table, filters, txn, Some(LockMode.LOCK_MODE_NL))
      // generate a file
      val files = workload_generator.genInsertFiles(table)

      // if the table is partitioned, get the corresponding partition
      if (table.partitionColumnNames.nonEmpty) {
        files.foreach { file =>
          if (txn.get.isOK()) {
            val partition = tree_cat.getPartition(table, file.partitionValues, txn,
              Some(LockMode.LOCK_MODE_X))
          }
        }
      }

      // finally add the file to the corresponding partition
      if (txn.get.isOK()) {
        tree_cat.addFiles(table, files, txn)
      }

      if (op_data.isDefined) {
        op_data.get.data_sent += txn.get.data_sent
        op_data.get.data_received += txn.get.data_received
      }

      tree_cat.commit(txn.get)
    }

    private def readOp(op_data : Option[OpData]): Boolean = {
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
      val table_name = table_config.name

      val table = tree_cat.getTable(database_name, table_name, txn, Some(LockMode.LOCK_MODE_NL))
      val filters = workload_generator.genRangeFilter()
      tree_cat.listFilesByFilter(table, filters, txn, Some(LockMode.LOCK_MODE_NL))

      if (op_data.isDefined) {
        op_data.get.data_sent += txn.get.data_sent
        op_data.get.data_received += txn.get.data_received
      }

      tree_cat.commit(txn.get)
    }

  }

}

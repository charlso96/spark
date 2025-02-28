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
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType

import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTableFile, CatalogTypes}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DateType, FractionalType, IntegralType, StructField}
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.tree.grpc.Grpccatalog.LockMode
import org.apache.spark.tree.grpc.Grpccatalog.TxnMode


//  class TableGenerator(fact_weights : Map[String, Int], dimension_weights : Map[String, Int]) {
//    val cumulative_fact_weights: List[(String, Int)] = fact_weights.toList.scanLeft(("", 0)) {
//      case ((_, cumulative), (string, weight)) => (string, cumulative + weight)
//    }.tail
//
//    private val total_fact_weight = cumulative_fact_weights.last._2
//
//    val cumulative_dimension_weights: List[(String, Int)] = dimension_weights.toList
//    .scanLeft(("", 0))
//    {
//      case ((_, cumulative), (string, weight)) => (string, cumulative + weight)
//    }.tail
//
//    private val total_dimension_weight = cumulative_dimension_weights.last._2
//
//    def genFactTable() : String = {
//      val random_val = ThreadLocalRandom.current().nextInt(total_fact_weight)
//      cumulative_fact_weights.find {
//        case (_, cumulative) => random_val < cumulative
//      }.map(_._1).get
//    }
//
//    def genDimensionTable() : String = {
//      val random_val = ThreadLocalRandom.current().nextInt(total_dimension_weight)
//      cumulative_dimension_weights.find {
//        case (_, cumulative) => random_val < cumulative
//      }.map(_._1).get
//    }
//
//  }


object ConcurrencyExperiment2 {
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("summaryOutput", "/tmp/concurrency2-summary.json")
  misc_config.put("opOutput", "/tmp/concurrency2-op.json")
  misc_config.put("dryRunTime", "00:00:30")
  misc_config.put("experimentTime", "00:05:00")
  misc_config.put("numThreads", "10")
  misc_config.put("totalNumThreads", "30")
  misc_config.put("version", "1")
  // Ratio is optimize:insertfact:insertdim:delete:read operations in order
  misc_config.put("workloadRatio", "2:288:24:1:315")
  misc_config.put("dbDist", "2:2:2:2:2")
  misc_config.put("scaleFactor", "100T")
  misc_config.put("treeAddress", "localhost:9876")
  misc_config.put("startDate", "1998-01-01")
  misc_config.put("endDate", "2003-12-31")

  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.ConcurrencyExperiment2 " +
        "<dataConfig> <workloadConfig>\n")
      return
    }

    val json_parser = new ObjectMapper
    // read in data config
    val data_config_json = json_parser.readTree(Source.fromFile(args(0)).mkString)
    // read in workload config
    val workload_config_json = json_parser.readTree(Source.fromFile(args(1)).mkString)

    val database_names = ArrayBuffer[String]()
    data_config_json.get("databaseNames").forEach { database_name =>
      database_names += database_name.asText()
    }

    // initialize misc config map
    workload_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val db_dist: ArrayBuffer[Int] = ArrayBuffer(misc_config("dbDist").split(":").map(_.toInt): _*)
    val optimize_config = new OptimizeConfig(workload_config_json.get("optimize"))
    val insert_config = new InsertConfig(workload_config_json.get("insert"))
    val delete_config = new DeleteConfig(workload_config_json.get("delete"))
    val read_config = new ReadConfig(workload_config_json.get("read"))

    val table_configs = scala.collection.mutable.HashMap[String, TableConfig]()
    data_config_json.get("tables").forEach { table_json =>
      table_configs.put(table_json.get("name").asText(), new TableConfig(table_json,
        misc_config("scaleFactor")))
    }

    // initialize the dates
    val date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start_date = LocalDate.parse(misc_config("startDate"), date_formatter)
    val end_date = LocalDate.parse(misc_config("endDate"), date_formatter)
    val dates: Array[String] = Iterator.iterate(start_date)(_ plusDays 1) // Generate dates
      .takeWhile(!_.isAfter(end_date)) // Stop when exceeding the end date
      .map(_.format(date_formatter)) // Convert to formatted strings
      .toArray

    // Initialize the table generator. Probabilities are weighed equally for now
    val table_generator = new TableGenerator(insert_config, optimize_config,
      delete_config, read_config)
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
      val idx = db_dist.indexWhere(_ != 0)
      if (idx != -1) {
        db_dist(idx) -= 1
        val op_data_array = ArrayBuffer[OpData]()
        total_op_data.append(op_data_array)
        threads.append(new Thread(new threadOps(database_names(idx), misc_config.toMap,
          optimize_config, insert_config, delete_config, read_config,
          table_configs.toMap, dates, table_generator, op_generator, exec_dry_run,
          exec_experiment, total_num_commits, total_num_aborts, op_data_array)))
      }
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

  private class threadOps(database_name: String, misc_config: Map[String, String],
                          optimize_config : OptimizeConfig, insert_config: InsertConfig,
                          delete_config : DeleteConfig, read_config: ReadConfig,
                          table_configs: Map[String, TableConfig],
                          dates: Array[String], table_generator: TableGenerator, op_generator:
                          OpGenerator, exec_dry_run: AtomicBoolean, exec_experiment: AtomicBoolean,
                          total_num_commits: AtomicLong, total_num_aborts: AtomicLong,
                          op_data_array : ArrayBuffer[OpData])
    extends Runnable {

    private val tree_address = misc_config("treeAddress")

    private val tree_cat = new TreeExternalCatalog(tree_address)

    private val sqlParser = new SparkSqlParser()

    private var num_commits = 0
    private var num_aborts = 0

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
        case 0 => optimizeOp(op_data)
        case 1 => insertFact(op_data)
        case 2 => insertDim(op_data)
        case 3 => deleteOp(op_data)
        case _ => readOp(op_data)
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

    private def dummyOp(): Boolean = {
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      tree_cat.commit(txn.get)
    }

    private def optimizeOp(op_data : Option[OpData]): Boolean = {
      val read_txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
      val write_txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)

      val target_table_name = table_generator.genOptimizeTable()
      val target_table_config = table_configs(target_table_name)
      // get the target fact table. Is part of read-write txn as schema etc. should not change.
      val target_table = tree_cat.getTable(database_name, target_table_config.name, write_txn,
        Some(LockMode.LOCK_MODE_X))
      // get the list of files to compact. Use read txn to avoid unnecessary conflicts
      val old_files = tree_cat.listFilesToCompact(target_table, optimize_config.threshold.toLong,
        read_txn)

      val partition_groups = scala.collection.mutable.HashMap[CatalogTypes.TablePartitionSpec,
        ArrayBuffer[CatalogTableFile]]()
      // categorize the older files to appropriate partition groups
      old_files.foreach { old_file =>
        val partition_group = partition_groups.get(old_file.partitionValues)
        if (partition_group.isDefined) {
          partition_group.get += old_file
        }
        else {
          val new_partition_group = ArrayBuffer[CatalogTableFile]()
          new_partition_group += old_file
          partition_groups.put(old_file.partitionValues, new_partition_group)
        }
      }

      val merged_files = ArrayBuffer[CatalogTableFile]()
      val new_files = ArrayBuffer[CatalogTableFile]()
      for (partition_group <- partition_groups.values) {
        if (partition_group.length > 1) {
          var new_file = switchUUID(partition_group(0), UUID.randomUUID())
          merged_files += partition_group(0)
          for (i <- 1 until partition_group.length) {
            if (new_file.size < optimize_config.threshold) {
              new_file = mergeFiles(target_table, new_file, partition_group(i))
            }
            else {
              new_files += new_file
              new_file = switchUUID(partition_group(i), UUID.randomUUID())
            }
            merged_files += partition_group(i)
          }
          new_files += new_file
        }
      }

      if (write_txn.get.isOK()) {
        tree_cat.removeFiles(target_table, merged_files, write_txn)
        tree_cat.addFiles(target_table, new_files, write_txn)
      }

      if (op_data.isDefined) {
        op_data.get.data_sent += read_txn.get.data_sent
        op_data.get.data_received += read_txn.get.data_received
        op_data.get.data_sent += write_txn.get.data_sent
        op_data.get.data_received += write_txn.get.data_received
      }

      tree_cat.commit(write_txn.get)

    }

    private def insertFact(op_data : Option[OpData]) : Boolean = {
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      // randomly choose a fact table, using the table generator
      val dest_fact_table_name = table_generator.genInsertFactTable()
      val dest_fact_table_config = table_configs(dest_fact_table_name)
      // get the target fact table in IX mode
      val dest_fact_table = tree_cat.getTable(database_name, dest_fact_table_config.name, txn,
        Some(LockMode.LOCK_MODE_IX))
      // column statistics
      val col_stats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
      dest_fact_table_config.schema.foreach { attr_config =>
        if (attr_config.key.isDefined) {
          val dim_table_config = table_configs(attr_config.key.get)
          val cardinality = dim_table_config.getWatermark().toInt
          var min_sk = ThreadLocalRandom.current().nextInt(cardinality)
          var max_sk = min_sk
          val rand_sk = ThreadLocalRandom.current().nextInt(cardinality)
          if (rand_sk < min_sk) {
            min_sk = rand_sk
          }
          if (rand_sk > max_sk) {
            max_sk = rand_sk
          }

          if (dim_table_config.business_id.isDefined && txn.get.isOK()) {
            val filters = ArrayBuffer[Expression]()
            val min_id = skToId(16, min_sk)
            val max_id = skToId(16, max_sk)
            val pred = f"${dim_table_config.business_id.get.name} >= '$min_id' and " +
              f"${dim_table_config.business_id.get.name} <= '$max_id'"
            filters.append(sqlParser.parseExpression(pred))
            // get the dimension table
            val table = tree_cat.getTable(database_name, dim_table_config.name, txn,
              Some(LockMode.LOCK_MODE_IS))
            // get the list of files from the dimension table
            if (txn.get.isOK()) {
              tree_cat.listFilesByFilter(table, filters, txn, Some(LockMode.LOCK_MODE_S))
            }
          }
          else if (txn.get.isOK()) {
            // just get all the files of the dimension table
            val table = tree_cat.getTable(database_name, dim_table_config.name, txn,
              Some(LockMode.LOCK_MODE_IS))
            // get the list of files from the dimension table
            if (txn.get.isOK()) {
              tree_cat.listFiles(table, txn, Some(LockMode.LOCK_MODE_S))
            }
          }

          val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
            Some(BigInt(0)), None, None, None, 1)
          col_stats.put(attr_config.name, col_stat)

        }
        // for other attributes, generate random values and fill in the column statistics
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
            case "DATE" =>
              min = dates(ThreadLocalRandom.current().nextInt(dates.length))
              val temp = dates(ThreadLocalRandom.current().nextInt(dates.length))
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
              min = ThreadLocalRandom.current().nextInt(attr_config.cardinality.get.toInt).toString
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
      val row_count = (insert_config.insert_ratio * dest_fact_table_config.num_rows).toInt
      val size_in_bytes = row_count * dest_fact_table_config.bytes_per_row
      val file_stats = CatalogStatistics(size_in_bytes, Some(BigInt(row_count)), col_stats.toMap)

      val dest_part_spec = scala.collection.mutable.Map[String, String]()
      dest_fact_table.partitionSchema.indices.foreach { i =>
        val partition_col = dest_fact_table.partitionSchema(i)
        // partition col follows uniform distribution for now
        if (partition_col.dataType.isInstanceOf[IntegralType]) {
          dest_part_spec.put(partition_col.name,
            ThreadLocalRandom.current().nextInt(dates.length).toString)
        }
        // partition col follows uniform distribution for now
        if (partition_col.dataType == DateType) {
          dest_part_spec.put(partition_col.name,
            dates(ThreadLocalRandom.current().nextInt(dates.length)))
        }
      }

      val immutable_dest_part_spec = dest_part_spec.toMap
      val fact_table_files = ArrayBuffer[CatalogTableFile]()
      val file_path = dest_fact_table.location.getPath + "/" + UUID.randomUUID()
      val storage = CatalogStorageFormat(Some(new URI(file_path)),
        dest_fact_table.storage.inputFormat, dest_fact_table.storage.outputFormat,
        dest_fact_table.storage.serde, false, dest_fact_table.properties)
      fact_table_files.append(CatalogTableFile(storage, immutable_dest_part_spec,
        file_stats.sizeInBytes.toLong, stats = Some(file_stats)))
      // if the fact table is partitioned, get the corresponding partition
      if (dest_fact_table.partitionColumnNames.nonEmpty && txn.get.isOK()) {
        val partition = tree_cat.getPartition(dest_fact_table, immutable_dest_part_spec, txn,
          Some(LockMode.LOCK_MODE_X))
      }

      // finally add the file to the chosen partition
      if (txn.get.isOK()) {
        tree_cat.addFiles(dest_fact_table, fact_table_files, txn)
      }

      if (op_data.isDefined) {
        op_data.get.data_sent += txn.get.data_sent
        op_data.get.data_received += txn.get.data_received
      }

      tree_cat.commit(txn.get)
    }

    private def insertDim(op_data : Option[OpData]) : Boolean = {
      // generate dimension tables
      val dest_dim_table_names = table_generator.genInsertDimTables()
      // increment the identity sk as an independent transaction first
      val min_id_sk = scala.collection.mutable.Map[String, Long]()
      val max_id_sk = scala.collection.mutable.Map[String, Long]()
      dest_dim_table_names.foreach { dest_dim_table_name =>
        val dest_dim_table_config = table_configs(dest_dim_table_name)
        val num_new_dim_records = (insert_config.insert_ratio * dest_dim_table_config
          .num_rows).toLong
        val min_sk = tree_cat.fetchAddAttr(database_name, dest_dim_table_config.name,
          dest_dim_table_config.sk.get.name, num_new_dim_records).get.toInt + 1
        val max_sk = min_sk + num_new_dim_records
        min_id_sk(dest_dim_table_name) = min_sk
        max_id_sk(dest_dim_table_name) = max_sk
      }

      // start of insert operation
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)

      dest_dim_table_names.foreach { dest_dim_table_name =>
        val dest_dim_table_config = table_configs(dest_dim_table_name)
        if (txn.get.isOK()) {
          // get the target dim table
          val dest_dim_table = tree_cat.getTable(database_name, dest_dim_table_config.name, txn,
            Some(LockMode.LOCK_MODE_X))
          if (txn.get.isOK()) {
            val dest_table_new_files = ArrayBuffer[CatalogTableFile]()
            // column statistics
            val col_stats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
            dest_dim_table_config.schema.foreach { attr_config =>
              if (attr_config.key.isDefined) {
                val dim_table_config = table_configs(attr_config.key.get)
                val cardinality = dim_table_config.getWatermark().toInt
                // range of sk that are written out
                var min_sk = ThreadLocalRandom.current().nextInt(cardinality)
                var max_sk = min_sk

                // increment the watermark only once, for the surrogate key
                if (dest_dim_table_name == attr_config.key.get) {
                  if (attr_config.name.endsWith("_sk")) {
                    // for destination dimension table, write range is precisely
                    // the newly allocated range of sks
                    min_sk = min_id_sk(dest_dim_table_name).toInt
                    max_sk = max_id_sk(dest_dim_table_name).toInt
                  }
                }
                else {
                  val rand_sk = ThreadLocalRandom.current().nextInt(cardinality)
                  if (rand_sk < min_sk) {
                    min_sk = rand_sk
                  }
                  if (rand_sk > max_sk) {
                    max_sk = rand_sk
                  }

                }

                if (attr_config.data_type == "DATE") {
                  // adjust the date range according to the cardinality
                  min_sk = min_sk * dates.length / cardinality
                  max_sk = max_sk * dates.length / cardinality
                  val col_stat = CatalogColumnStat(None, Some(dates(min_sk)),
                    Some(dates(max_sk)),
                    Some(BigInt(0)), None, None, None, 1)
                  col_stats.put(attr_config.name, col_stat)
                }
                else if (dim_table_config.business_id.isDefined) {
                  val filters = ArrayBuffer[Expression]()
                  val min_id = skToId(16, min_sk)
                  val max_id = skToId(16, max_sk)
                  val pred = f"${dim_table_config.business_id.get.name} >= '$min_id' and " +
                    f"${dim_table_config.business_id.get.name} <= '$max_id'"
                  filters.append(sqlParser.parseExpression(pred))

                  if (dest_dim_table_name == attr_config.key.get) {
                    if (attr_config.name.endsWith("_sk") && txn.get.isOK()) {
                      // get the dimension table
                      val table = tree_cat.getTable(database_name, dim_table_config.name, txn,
                        Some(LockMode.LOCK_MODE_IS))
                      if (txn.get.isOK()) {
                        tree_cat.listFilesByFilter(table, filters, txn, Some(LockMode.LOCK_MODE_X))
                        // populate column statistics for both surrogate key and business id
                        val col_stat = CatalogColumnStat(None, Some(min_sk.toString),
                          Some(max_sk.toString), Some(BigInt(0)), None, None, None, 1)
                        col_stats.put(attr_config.name, col_stat)
                        val id_col_stat = CatalogColumnStat(None, Some(min_id),
                          Some(max_id), Some(BigInt(0)), None, None, None, 1)
                        col_stats.put(attr_config.name.stripSuffix("_sk") + "_id", id_col_stat)
                      }
                    }
                  }
                  else if (txn.get.isOK()) {
                    // get the dimension table
                    val table = tree_cat.getTable(database_name, dim_table_config.name, txn,
                      Some(LockMode.LOCK_MODE_IS))
                    if (txn.get.isOK()) {
                      // get the list of files from the dimension table
                      tree_cat.listFilesByFilter(table, filters, txn, Some(LockMode.LOCK_MODE_S))
                      val col_stat = CatalogColumnStat(None, Some(min_sk.toString),
                        Some(max_sk.toString), Some(BigInt(0)), None, None, None, 1)
                      col_stats.put(attr_config.name, col_stat)
                    }
                  }
                }
                else if (txn.get.isOK()) {
                  val table = tree_cat.getTable(database_name, dim_table_config.name, txn,
                    Some(LockMode.LOCK_MODE_IS))
                  if (txn.get.isOK()) {
                    // just get all the files of the dimension table
                    tree_cat.listFiles(table, txn, Some(LockMode.LOCK_MODE_S))
                    val col_stat = CatalogColumnStat(None, Some(min_sk.toString),
                      Some(max_sk.toString), Some(BigInt(0)), None, None, None, 1)
                    col_stats.put(attr_config.name, col_stat)
                  }
                }
              }
              // for other attributes, generate random values and fill in the column statistics
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
                  case "DATE" =>
                    min = dates(ThreadLocalRandom.current().nextInt(dates.length))
                    val temp = dates(ThreadLocalRandom.current().nextInt(dates.length))
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
                    min = ThreadLocalRandom.current().nextInt(attr_config.cardinality.get.toInt)
                      .toString
                    val temp = ThreadLocalRandom.current().nextInt(attr_config.cardinality.get
                      .toInt).toString
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

            if (txn.get.isOK()) {
              // Now, insert to dim table a single file with appropriate number of rows, file stats
              // etc.
              val row_count = (insert_config.insert_ratio * dest_dim_table_config.num_rows).toInt
              val size_in_bytes = row_count * dest_dim_table_config.bytes_per_row
              val file_stats = CatalogStatistics(size_in_bytes, Some(BigInt(row_count)),
                col_stats.toMap)

              val dest_part_spec = scala.collection.mutable.Map[String, String]()
              dest_dim_table.partitionSchema.indices.foreach { i =>
                val partition_col = dest_dim_table.partitionSchema(i)
                // partition col follows uniform distribution for now
                if (partition_col.dataType.isInstanceOf[IntegralType]) {
                  dest_part_spec.put(partition_col.name,
                    ThreadLocalRandom.current().nextInt(dates.length).toString)
                }
                // partition col follows uniform distribution for now
                if (partition_col.dataType == DateType) {
                  dest_part_spec.put(partition_col.name,
                    dates(ThreadLocalRandom.current().nextInt(dates.length)))
                }
              }

              val immutable_dest_part_spec = dest_part_spec.toMap
              val file_path = dest_dim_table.location.getPath + "/" + UUID.randomUUID()
              val storage = CatalogStorageFormat(Some(new URI(file_path)),
                dest_dim_table.storage.inputFormat, dest_dim_table.storage.outputFormat,
                dest_dim_table.storage.serde, false, dest_dim_table.properties)
              dest_table_new_files.append(CatalogTableFile(storage, immutable_dest_part_spec,
                file_stats.sizeInBytes.toLong, stats = Some(file_stats)))
              // finally add batch of files to the dest dimension table
              tree_cat.addFiles(dest_dim_table, dest_table_new_files, txn)
            }
          }
        }
      }

      val success = tree_cat.commit(txn.get)

      max_id_sk.foreach { entry =>
        table_configs(entry._1).setWatermark(entry._2)
      }

      if (op_data.isDefined) {
        op_data.get.data_sent += txn.get.data_sent
        op_data.get.data_received += txn.get.data_received
      }

      success
    }

    private def deleteOp(op_data : Option[OpData]): Boolean = {
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      val target_table_names = table_generator.genDeleteTable()

      val min_sk = ThreadLocalRandom.current().nextInt(dates.length)
      val max_sk = min_sk + delete_config.date_range - 1
      val min_sk_str = "%012d".format(min_sk)
      val max_sk_str = "%012d".format(max_sk)
      target_table_names.foreach{ target_table_name =>
        if (txn.get.isOK()) {
          // get the target fact table.
          val target_table = tree_cat.getTable(database_name, target_table_name, txn,
            Some(LockMode.LOCK_MODE_IX))
          val partition_column = target_table.partitionSchema.head
          val partition_col_name = partition_column.name
          val filters = ArrayBuffer[Expression]()
          val pred = f"$partition_col_name >= '$partition_col_name=$min_sk_str' and " +
            f"$partition_col_name <= '$partition_col_name=$max_sk_str'"
          filters.append(sqlParser.parseExpression(pred))

          if (txn.get.isOK()) {
            val files = tree_cat.listFilesWithStatsByFilter(target_table, filters, txn,
              Some(LockMode.LOCK_MODE_X))
            tree_cat.removeFiles(target_table, files, txn)
          }
        }

      }

      if (op_data.isDefined) {
        op_data.get.data_sent += txn.get.data_sent
        op_data.get.data_received += txn.get.data_received
      }

      tree_cat.commit(txn.get)
    }

    private def readOp(op_data : Option[OpData]): Boolean = {
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
      val query_config = table_generator.genReadQuery()
      val table_jsons = query_config._1.elements().next()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val table = tree_cat.getTable(database_name, table_name, txn,
            Some(LockMode.LOCK_MODE_NL))
          tree_cat.listFiles(table, txn, Some(LockMode.LOCK_MODE_NL))
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val table = tree_cat.getTable(database_name, table_name, txn,
            Some(LockMode.LOCK_MODE_NL))
          val partition_column = table.partitionSchema.head
          val partition_col_name = partition_column.name
          val filters = ArrayBuffer[Expression]()

          val partition_range = table_json.get("range")

          if (partition_range.size() == 1) {
            val sk = "%012d".format(partition_range.get(0).asInt())
            val pred = f"$partition_col_name == '$partition_col_name=$sk'"
            filters.append(sqlParser.parseExpression(pred))
          }
          else if (partition_range.size() == 2) {
            val min_sk = "%012d".format(partition_range.get(0).asInt())
            val max_sk = "%012d".format(partition_range.get(1).asInt())
            val pred = f"$partition_col_name >= '$partition_col_name=$min_sk' and " +
              f"$partition_col_name <= '$partition_col_name=$max_sk'"
            filters.append(sqlParser.parseExpression(pred))
          }

          tree_cat.listFilesByFilter(table, filters, txn, Some(LockMode.LOCK_MODE_NL))

        }

      }

      if (op_data.isDefined) {
        op_data.get.misc = query_config._2.toString
        op_data.get.data_sent += txn.get.data_sent
        op_data.get.data_received += txn.get.data_received
      }

      tree_cat.commit(txn.get)

    }
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

  private def skToId(length : Int, sk : Int) : String = {
    val char_buf = ArrayBuffer.fill(length)('A')
    var quotient = sk
    var cur_length = 0
    while (quotient > 0) {
      val remainder = quotient % 26
      quotient = quotient / 26
      cur_length += 1
      char_buf(length - cur_length) = (remainder + 65).toChar
    }

    char_buf.mkString

  }

  private def convertToMilliseconds(time : String): Int = {
    val parts = time.split(":").map(_.toInt)
    val hours = parts(0)
    val minutes = parts(1)
    val seconds = parts(2)
    (hours * 3600 + minutes * 60 + seconds) * 1000
  }

  private def switchUUID(file: CatalogTableFile, uuid: UUID): CatalogTableFile = {
    val old_file_path = file.storage.locationUri.get.toString
    val new_file_path = Some(new URI(old_file_path.substring(0, old_file_path.lastIndexOf('/')
      + 1) + uuid))
    val file_storage = file.storage
    val new_storage = CatalogStorageFormat(new_file_path, file_storage.inputFormat,
      file_storage.outputFormat, file_storage.serde, file_storage.compressed,
      file_storage.properties)
    CatalogTableFile(new_storage, file.partitionValues, file.size, System.currentTimeMillis,
      file.stats, file.tags)
  }

  // merges src_file to dest_file, including all the stats etc.
  private def mergeFiles(table: CatalogTable, dest_file: CatalogTableFile,
                         src_file: CatalogTableFile): CatalogTableFile = {
    val stats = mergeStats(table, dest_file.stats.get, src_file.stats.get)
    CatalogTableFile(dest_file.storage, dest_file.partitionValues, dest_file.size + src_file.size,
      System.currentTimeMillis, Some(stats), dest_file.tags)
  }

  private def mergeStats(table : CatalogTable, base : CatalogStatistics,
                         delta: CatalogStatistics): CatalogStatistics = {
    val sizeInBytes = base.sizeInBytes + delta.sizeInBytes
    val rowCount = {
      if (base.rowCount.isDefined && delta.rowCount.isDefined) {
        Some(base.rowCount.get + delta.rowCount.get)
      }
      else {
        None
      }
    }
    val colStats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
    table.schema.foreach { attr =>
      val mergedColStat = mergeColStats(attr, base.colStats.get(attr.name),
        delta.colStats.get(attr.name))
      if (mergedColStat.isDefined) {
        colStats.put(attr.name, mergedColStat.get)
      }
    }
    CatalogStatistics(sizeInBytes, rowCount, colStats.toMap)
  }

  // we assume that delta is always more "complete" than base
  private def mergeColStats(attr: StructField, base: Option[CatalogColumnStat],
                            delta: Option[CatalogColumnStat]) : Option[CatalogColumnStat] = {

    if (base.isDefined) {
      if (delta.isDefined) {
        val min = {
          if (base.get.min.isDefined && delta.get.min.isDefined) {
            attr.dataType match {
              case _: IntegralType =>
                Some(base.get.min.get.toInt.min(delta.get.min.get.toInt).toString)
              case _: FractionalType =>
                val min_val = base.get.min.get.toDouble.max(delta.get.min.get.toDouble)
                Some(f"$min_val%.2f")
              case _ =>
                Some(if (base.get.min.get < delta.get.min.get) base.get.min.get
                else delta.get.min.get)
            }
          }
          else {
            None
          }
        }
        val max = {
          if (base.get.max.isDefined && delta.get.max.isDefined) {
            attr.dataType match {
              case _: IntegralType =>
                Some(base.get.max.get.toInt.max(delta.get.max.get.toInt).toString)
              case _: FractionalType =>
                val max_val = base.get.max.get.toDouble.max(delta.get.max.get.toDouble)
                Some(f"$max_val%.2f")
              case _ =>
                Some(if (base.get.max.get > delta.get.max.get) base.get.max.get
                else delta.get.max.get)
            }
          }
          else {
            None
          }
        }
        val nullCount = {
          if (base.get.nullCount.isDefined && delta.get.nullCount.isDefined) {
            Some(base.get.nullCount.get + delta.get.nullCount.get)
          }
          else {
            None
          }
        }
        Some(CatalogColumnStat(None, min, max, nullCount, None, None, None, 1))
      }
      else {
        base
      }
    }
    else if (delta.isDefined) {
      delta
    }
    else {
      None
    }
  }

}
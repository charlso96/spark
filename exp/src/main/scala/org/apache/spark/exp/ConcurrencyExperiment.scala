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
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType

import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTableFile, CatalogTypes}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DateType, FractionalType, IntegralType, StructField}
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.tree.grpc.Grpccatalog.TxnMode


sealed trait TableType
object TableType {
  case object Fact extends TableType
  case object Dimension extends TableType
}

class AttrConfig(attr_json: JsonNode) {
  val name : String = attr_json.get("name").asText()
  val data_type : String = attr_json.get("type").asText()
  val key : Option[String] = attr_json.get("cardinality").getNodeType match {
    case JsonNodeType.STRING => Some(attr_json.get("cardinality").asText())
    case _ => None
  }
  val cardinality : Option[Long] = attr_json.get("cardinality").getNodeType match {
    case JsonNodeType.STRING => None
    case _ => Some(attr_json.get("cardinality").asLong())
  }
  val clustered : Boolean = attr_json.get("clustered").asBoolean()
}

class TableConfig(table_json: JsonNode, scale_factor : String) {
  val name: String = table_json.get("name").asText()
  val table_type: TableType = if (table_json.get("type").asText() == "dimension") {
    TableType.Dimension
  }
  else {
    TableType.Fact
  }
  val partition_schema : Array[AttrConfig] = deSerSchema(table_json.get("partitionSchema"))
  val schema : Array[AttrConfig] = deSerSchema(table_json.get("schema"))
  val bytes_per_row : Long = 4*(partition_schema.length + schema.length)
  val num_rows : Long = table_json.get("scaling").get(scale_factor).asLong()
  val high_watermark : AtomicLong = new AtomicLong(num_rows)
  val business_id : Option[AttrConfig] = schema.find{ attr_config => attr_config.name.
    endsWith("_id") && attr_config.clustered }

  private def deSerSchema(schema_json : JsonNode) : Array[AttrConfig] = {
    val schema = ArrayBuffer[AttrConfig]()
    schema_json.forEach { attr_json =>
      schema += new AttrConfig(attr_json)
    }
    schema.toArray
  }
}

class InsertConfig(insert_config_json: JsonNode) {
  val insert_ratio : Double = insert_config_json.get("insertRatio").asDouble
  val fact_tables : ArrayBuffer[String] = ArrayBuffer[String]()
  val dim_tables : ArrayBuffer[String] = ArrayBuffer[String]()
  val dimension_update_rates : ArrayBuffer[Double] = ArrayBuffer[Double]()
  insert_config_json.get("factTables").forEach { table =>
    fact_tables += table.asText()
  }
  insert_config_json.get("dimensionTables").forEach { table =>
    dim_tables += table.asText()
  }
  insert_config_json.get("dimensionUpdateRates").forEach { rate =>
    dimension_update_rates += rate.asDouble()
  }
}

class OptimizeConfig(optimize_config_json: JsonNode) {
  val threshold : Int = optimize_config_json.get("threshold").asInt()
  val tables : ArrayBuffer[String] = ArrayBuffer[String]()
  optimize_config_json.get("tables").forEach{ table =>
    tables += table.asText()
  }
}

class DeleteConfig(delete_config_json : JsonNode) {
  val date_range : Int = delete_config_json.get("dateRange").asInt()
  val tables : ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()
  delete_config_json.get("tables").forEach{ table_list =>
    val temp_array = ArrayBuffer[String]()
    table_list.forEach{ table =>
      temp_array += table.asText()
    }
    tables += temp_array.toArray
  }
}

class UpdateConfig(update_config_json : JsonNode) {
  val update_ratio : Double = update_config_json.get("updateRatio").asDouble
  val tables : ArrayBuffer[String] = ArrayBuffer[String]()
  update_config_json.get("tables").forEach{ table =>
    tables += table.asText()
  }
}

class TableGenerator(insert_config : InsertConfig, optimize_config : OptimizeConfig,
                      delete_config : DeleteConfig, update_config : UpdateConfig) {
  def genInsertFactTable(): String = {
    insert_config.fact_tables(ThreadLocalRandom.current().nextInt(insert_config.fact_tables.length))
  }

  def genInsertDimTables(): ArrayBuffer[String] = {
    val insert_dim_tables = ArrayBuffer[String]()
    var add_dimension = true
    var idx = 0
    // there are 2 tables, customer and customer address for now
    while (add_dimension && idx < insert_config.dim_tables.length) {
      add_dimension = (ThreadLocalRandom.current().nextDouble() < insert_config
        .dimension_update_rates(idx))
      if (add_dimension) {
        insert_dim_tables.append(insert_config.dim_tables(idx))
      }
      idx = idx + 1
    }

    insert_dim_tables
  }

  def genOptimizeTable() : String = {
    optimize_config.tables(ThreadLocalRandom.current().nextInt(optimize_config.tables.length))
  }

  def genDeleteTable() : Array[String] = {
    delete_config.tables(ThreadLocalRandom.current().nextInt(delete_config.tables.length))
  }

  def genUpdateTable() : String = {
    update_config.tables(ThreadLocalRandom.current().nextInt(update_config.tables.length))
  }

}


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


object ConcurrencyExperiment {
  private val sqlParser = new SparkSqlParser()
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("seed", "0")
  misc_config.put("summaryOutput", "/tmp/summary.txt")
  misc_config.put("latencyOutput", "/tmp/latency.txt")
  misc_config.put("dryRunTime", "00:00:20")
  misc_config.put("experimentTime", "00:00:60")
  misc_config.put("numThreads", "16")
  misc_config.put("workloadRatio", "0:2:288:1:1")
  misc_config.put("scaleFactor", "1G")
  misc_config.put("tableGenBase", "1.5")
  misc_config.put("treeAddress", "localhost:9876")
  misc_config.put("startDate", "1998-01-01")
  misc_config.put("endDate", "2003-12-31")

  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.ConcurrencyExperiment " +
        "<dataConfig> <workloadConfig>\n")
      return
    }

    val json_parser = new ObjectMapper
    // read in data config
    val data_config_json = json_parser.readTree(Source.fromFile(args(0)).mkString)
    // read in workload config
    val workload_config_json = json_parser.readTree(Source.fromFile(args(1)).mkString)
    val database_name = data_config_json.get("databaseName").asText()
    // initialize misc config map
    workload_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val optimize_config = new OptimizeConfig(workload_config_json.get("optimize"))
    val insert_config = new InsertConfig(workload_config_json.get("insert"))
    val delete_config = new DeleteConfig(workload_config_json.get("delete"))
    val update_config = new UpdateConfig(workload_config_json.get("update"))
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
      delete_config, update_config)

    val exec_dry_run = new AtomicBoolean(false)
    val exec_experiment = new AtomicBoolean(false)
    val total_num_commits = new AtomicLong(0)
    val total_num_aborts = new AtomicLong(0)
    val dry_run_time = convertToMilliseconds(misc_config("dryRunTime"))
    val experiment_time = convertToMilliseconds(misc_config("experimentTime"))

    val total_commit_latencies = ArrayBuffer[ArrayBuffer[Long]]()
    val total_abort_latencies = ArrayBuffer[ArrayBuffer[Long]]()
    val threads = ArrayBuffer[Thread]()

    for (i <- 0 until misc_config("numThreads").toInt) {
      val commit_latencies = ArrayBuffer[Long]()
      val abort_latencies = ArrayBuffer[Long]()
      total_commit_latencies.append(commit_latencies)
      total_abort_latencies.append(abort_latencies)
      threads.append(new Thread(new threadOps(database_name, misc_config.toMap, insert_config,
        optimize_config, table_configs.toMap, dates, table_generator, exec_dry_run,
        exec_experiment, total_num_commits, total_num_aborts, commit_latencies, abort_latencies)))
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

    val flatTotalCommitLatencies = total_commit_latencies.flatten
    val flatTotalAbortLatencies = total_abort_latencies.flatten
    val avgCommitLatency = if (flatTotalCommitLatencies.size > 0) {
      flatTotalCommitLatencies.sum / flatTotalCommitLatencies.size
    }
    else {
      0
    }

    // print the results
    val summaryWriter = new FileWriter(new File(misc_config("summaryOutput")), true)
    summaryWriter.write("\"numCommits\":" + total_num_commits + ", ")
    summaryWriter.write("\"numAborts\":" + total_num_aborts + ", ")
    summaryWriter.write("\"throughput\":" +
      (total_num_commits.get().toDouble * 1000) / experiment_time + ", ")
    summaryWriter.write("\"avgCommitLatency\":" + avgCommitLatency / 1000000 + ", ")
    summaryWriter.write("\"dryRunTime\":" + dry_run_time / 1000 + ", ")
    summaryWriter.write("\"experimentTime\":" + experiment_time / 1000 + ", ")
    summaryWriter.write("\"numThreads\":" + misc_config("numThreads") + ", ")
    summaryWriter.write("\n")
    summaryWriter.close()
    //
    val latencyWriter = new FileWriter(new File(misc_config("latencyOutput")), true)
    latencyWriter.write(flatTotalCommitLatencies.mkString("[", ",", "]"))
    latencyWriter.write(flatTotalAbortLatencies.mkString("[", ",", "]"))
    latencyWriter.write("\n")
    latencyWriter.close()

  }

  private class threadOps(database_name: String, misc_config: Map[String, String],
                          insert_config: InsertConfig, optimize_config : OptimizeConfig,
                          table_configs: Map[String, TableConfig],
                          dates: Array[String], table_generator: TableGenerator,
                          exec_dry_run: AtomicBoolean, exec_experiment: AtomicBoolean,
                          total_num_commits: AtomicLong, total_num_aborts: AtomicLong,
                          commit_latencies: ArrayBuffer[Long], abort_latencies: ArrayBuffer[Long])
    extends Runnable {

    private val tree_address = misc_config("treeAddress")

    private val tree_cat = new TreeExternalCatalog(tree_address)

    private val sqlParser = new SparkSqlParser()

    private var num_commits = 0
    private var num_aborts = 0

    override def run(): Unit = {
      while (exec_dry_run.get()) {
        runCycle(true)
      }

      while (exec_experiment.get()) {
        runCycle(false)
      }

      total_num_commits.addAndGet(num_commits)
      total_num_aborts.addAndGet(num_aborts)

    }

    // helper function for running a single cycle
    private def runCycle(is_dry_run : Boolean): Unit = {
       val success = if (ThreadLocalRandom.current().nextInt(100) < 2) {
           optimizeOp()
         }
         else {
           insertOp()
         }

//    val success = dummyOp()

      // if not dry run, collect the results
      if (!is_dry_run) {
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

    private def optimizeOp(): Boolean = {
      val read_txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_ONLY)
      val write_txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)

      val target_table_name = table_generator.genOptimizeTable()
      val target_table_config = table_configs(target_table_name)
      // get the target fact table. Is part of read-write txn as schema etc. should not change.
      val target_table = tree_cat.getTable(database_name, target_table_config.name, write_txn)
      // get the list of files to compact. Use read txn to avoid unnecessary conflicts
      val old_files = tree_cat.listFilesToCompact(target_table, optimize_config.threshold.toLong,
        read_txn)

      val partition_groups = scala.collection.mutable.HashMap[CatalogTypes.TablePartitionSpec,
        ArrayBuffer[CatalogTableFile]]()

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

      print("merged files\n")
      merged_files.foreach{ file =>
        print(file.toString + "\n")
      }

      print("new files\n")
      new_files.foreach{ file =>
        print(file.toString + "\n")
      }

      tree_cat.removeFiles(target_table, merged_files, write_txn)
      tree_cat.addFiles(target_table, new_files, write_txn)

      tree_cat.commit(write_txn.get)
    }

    private def insertOp(): Boolean = {
      // start of insert operation
      val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      // randomly choose a fact table, using the table generator
      val dest_fact_table_name = table_generator.genInsertFactTable()
      // decide whether to insert to dimension tables
      val dest_dim_table_names = table_generator.genInsertDimTables()

      val dest_fact_table_config = table_configs(dest_fact_table_name)
      // get the target fact table
      val dest_fact_table = tree_cat.getTable(database_name, dest_fact_table_config.name, txn)
      // column statistics
      val col_stats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
      dest_fact_table_config.schema.foreach { attr_config =>
        if (attr_config.key.isDefined) {
          val dim_table_config = table_configs(attr_config.key.get)
          val cardinality = dim_table_config.high_watermark.get.toInt
          var min_sk = ThreadLocalRandom.current().nextInt(cardinality)
          var max_sk = min_sk
          for (i <- 0 until (insert_config.insert_ratio * dest_fact_table_config.num_rows / 10)
            .toInt) {
            val rand_sk = ThreadLocalRandom.current().nextInt(cardinality)
            if (rand_sk < min_sk) {
              min_sk = rand_sk
            }
            if (rand_sk > max_sk) {
              max_sk = rand_sk
            }
          }

          if (dim_table_config.business_id.isDefined) {
            val filters = ArrayBuffer[Expression]()
            val min_id = skToId(16, min_sk)
            val max_id = skToId(16, max_sk)
            val pred = f"${dim_table_config.business_id.get.name} >= '$min_id' and " +
              f"${dim_table_config.business_id.get.name} <= '$max_id'"
            filters.append(sqlParser.parseExpression(pred))
            // get the dimension table
            val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
            // get the list of files from the dimension table
            tree_cat.listFilesByFilter(table, filters, txn)
          }
          else {
            // just get all the files of the dimension table
            val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
            // get the list of files from the dimension table
            tree_cat.listFiles(table, txn)
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
      if (dest_fact_table.partitionColumnNames.nonEmpty) {
        val partition = tree_cat.getPartition(dest_fact_table, immutable_dest_part_spec, txn)
      }

      // finally add the file to the chosen partition
      tree_cat.addFiles(dest_fact_table, fact_table_files, txn)

      dest_dim_table_names.foreach { dest_dim_table_name =>
        val dest_dim_table_config = table_configs(dest_dim_table_name)
        // get the target dim table
        val dest_dim_table = tree_cat.getTable(database_name, dest_dim_table_config.name, txn)
        // column statistics
        val col_stats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
        dest_dim_table_config.schema.foreach { attr_config =>
          if (attr_config.key.isDefined) {
            val dim_table_config = table_configs(attr_config.key.get)
            val cardinality = dim_table_config.high_watermark.get.toInt
            var min_sk = ThreadLocalRandom.current().nextInt(cardinality)
            var max_sk = min_sk

            // increment the watermark only once, for the surrogate key
            if (dest_dim_table_name == attr_config.key.get) {
              if (attr_config.name.endsWith("_sk")) {
                val num_new_dim_records = (insert_config.insert_ratio * dest_dim_table_config
                  .num_rows).toLong
                min_sk = dest_dim_table_config.high_watermark.getAndAdd(num_new_dim_records).toInt
                max_sk = (min_sk + num_new_dim_records - 1).toInt
              }
            }
            else {
              for (i <- 0 until (insert_config.insert_ratio * dest_fact_table_config.num_rows / 10)
                .toInt) {
                val rand_sk = ThreadLocalRandom.current().nextInt(cardinality)
                if (rand_sk < min_sk) {
                  min_sk = rand_sk
                }
                if (rand_sk > max_sk) {
                  max_sk = rand_sk
                }
              }
            }

            if (attr_config.data_type == "DATE") {
              min_sk = min_sk * dates.length / cardinality
              max_sk = max_sk * dates.length / cardinality
              val col_stat = CatalogColumnStat(None, Some(dates(min_sk)), Some(dates(max_sk)),
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
                // populate column statistics for both surrogate key and business id
                if (attr_config.name.endsWith("_sk")) {
                  // get the dimension table
                  val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
                  // get the list of files from the dimension table
                  tree_cat.listFilesByFilter(table, filters, txn)
                  val col_stat = CatalogColumnStat(None, Some(min_sk.toString),
                    Some(max_sk.toString), Some(BigInt(0)), None, None, None, 1)
                  col_stats.put(attr_config.name, col_stat)
                  val id_col_stat = CatalogColumnStat(None, Some(min_id), Some(max_id),
                    Some(BigInt(0)), None, None, None, 1)
                  col_stats.put(attr_config.name.stripSuffix("_sk") + "_id", id_col_stat)
                }
              }
              else {
                // get the dimension table
                val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
                // get the list of files from the dimension table
                tree_cat.listFilesByFilter(table, filters, txn)
                val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
                  Some(BigInt(0)), None, None, None, 1)
                col_stats.put(attr_config.name, col_stat)
              }
            }
            else {
              val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
              // just get all the files of the dimension table
              tree_cat.listFiles(table, txn)
              val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
                Some(BigInt(0)), None, None, None, 1)
              col_stats.put(attr_config.name, col_stat)
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
        // Now, insert to dim table a single file with appropriate number of rows, file stats etc.
        val row_count = (insert_config.insert_ratio * dest_dim_table_config.num_rows).toInt
        val size_in_bytes = row_count * dest_dim_table_config.bytes_per_row
        val file_stats = CatalogStatistics(size_in_bytes, Some(BigInt(row_count)), col_stats.toMap)

        val immutable_dest_part_spec = dest_part_spec.toMap
        val dest_table_files = ArrayBuffer[CatalogTableFile]()
        val file_path = dest_dim_table.location.getPath + "/" + UUID.randomUUID()
        val storage = CatalogStorageFormat(Some(new URI(file_path)),
          dest_dim_table.storage.inputFormat, dest_dim_table.storage.outputFormat,
          dest_dim_table.storage.serde, false, dest_dim_table.properties)
        dest_table_files.append(CatalogTableFile(storage, immutable_dest_part_spec,
          file_stats.sizeInBytes.toLong, stats = Some(file_stats)))
        // finally add batch of files to the dest dimension table
        tree_cat.addFiles(dest_dim_table, dest_table_files, txn)

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

  // * Randomly choose a fact table, using the table generator
  // * Randomly choose a dimension tables, using probability distributions defined in the
  // insertConfig
  // * For each dimension table that needs to join with the input data, generate the min
  // and max range of surrogate_ids & business_ids.
  // * For each dimension table, scan it with the predicate on the min, max range on the business
  // ids.
  // * Insert to fact table a single file with appropriate number of rows, file stats etc.
  // * If dimension tables are also selected for insert, insert to dimension table a single
  // file with appropriate min and max on business_ids and update the high watermark.

}
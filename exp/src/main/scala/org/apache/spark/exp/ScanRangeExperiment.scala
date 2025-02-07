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
import java.time.{Duration, Instant}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.util.internal.ThreadLocalRandom
import org.apache.iceberg.spark.source.SparkTable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, SparkExpressionConverter}

object ScanRangeExperiment {
  private val sql_parser = new SparkSqlParser()
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("resultOutput", "/tmp/scanexperiment.json")
  misc_config.put("experimentIters", "100")
  misc_config.put("treeAddress", "localhost:9876")
  misc_config.put("startDate", "1998-01-01")
  misc_config.put("endDate", "2003-12-31")
  misc_config.put("partitionRange", "7")
  misc_config.put("seed", "0")

  def main(args: Array[String]): Unit = {
    if (args.size != 3) {
      print("Usage: spark-class org.apache.spark.exp.ScanExperiment " +
        "<scanConfig> <catalogType> <numFiles>\n")
      return
    }

    val json_parser = new ObjectMapper
    // read in data config
    val scan_config_json = json_parser.readTree(Source.fromFile(args(0)).mkString)
    scan_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val result_output = misc_config("resultOutput")
    val iters = misc_config("experimentIters").toInt
    val tree_address = misc_config("treeAddress")

    val delta_db = scan_config_json.get("delta").asText()
    val hms_db = scan_config_json.get("hms").asText()
    val tree_db = scan_config_json.get("tree").asText()
    val iceberg_db = scan_config_json.get("iceberg").asText()
    val table_name = scan_config_json.get("tables").get(0).get("name").asText()
    val partition_name = scan_config_json.get("tables").get(0).get("partitionSchema").get(0)
      .get("name").asText()

    val catalog_type = args(1)
    val num_files = args(2).toInt

    // initialize the dates
    val date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start_date = LocalDate.parse(misc_config("startDate"), date_formatter)
    val end_date = LocalDate.parse(misc_config("endDate"), date_formatter)
    val dates: Array[String] = Iterator.iterate(start_date)(_ plusDays 1) // Generate dates
      .takeWhile(!_.isAfter(end_date)) // Stop when exceeding the end date
      .map(_.format(date_formatter)) // Convert to formatted strings
      .toArray
    val partition_range = misc_config("partitionRange").toInt
    val num_partitions = dates.length.min(num_files)
    ThreadLocalRandom.current().setSeed(misc_config("seed").toLong)
    // generate min sks.
    val min_sks = ArrayBuffer[Int]()
    for (i <- 0 until (iters + 10)) {
      min_sks += ThreadLocalRandom.current().nextInt(num_partitions - partition_range)
    }

    catalog_type match {
      case "delta" => scanDelta(result_output, iters, delta_db, table_name, partition_name,
        num_files, partition_range, min_sks)
      case "hms" => scanHMS(result_output, iters, hms_db, table_name, partition_name, num_files,
        partition_range, min_sks)
      case "iceberg" => scanIceberg(result_output, iters, iceberg_db, table_name, partition_name,
        num_files, partition_range, min_sks)
      case "tree" => scanTree(result_output, iters, tree_db, table_name, partition_name,
         num_files, partition_range, min_sks, tree_address)
      case _ => print("Invalid Catalog Type!!!")
    }
  }

  private def writeOutput(result_output : String, times : Seq[Long], catalog : String,
                          num_files : Int): Unit = {
    val output_writer = new FileWriter(new File(result_output), true)
    times.foreach { time =>
      output_writer.write("{\"catalog\":\"" + catalog + "\", ")
      output_writer.write("\"num_files\":" + num_files + ", ")
      output_writer.write("\"time\":" + time + "}")
      output_writer.write("\n")
    }
    output_writer.flush()
    output_writer.close()
  }

  private def constructHMSFilters(partition_name : String, min_sk: Int, offset: Int) :
    Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val partition_pred = f"${partition_name} >= $min_sk and " +
      f"${partition_name} < ${min_sk + offset}"
    filters.append(sql_parser.parseExpression(partition_pred))

    filters
  }

  private def scanDelta(result_output : String, iters : Int, db_name : String,
                        table_name : String, partition_name : String, num_files : Int,
                        partition_range : Int, min_sks : Seq[Int]) : Unit = {

    val delta_util = new DeltaUtil()
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructHMSFilters(partition_name, min_sk, partition_range)
      val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
      val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(filters, Seq.empty)
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructHMSFilters(partition_name, min_sk, partition_range)

      val start_time = Instant.now()
      val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
      val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(filters, Seq.empty)
      val end_time = Instant.now()

      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "delta", num_files)
  }

  private def scanHMS(result_output : String, iters : Int, db_name : String,
                      table_name : String, partition_name : String, num_files : Int,
                      partition_range : Int, min_sks : Seq[Int]) : Unit = {

    val hms_util = new HMSUtil()
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructHMSFilters(partition_name, min_sk, partition_range)

      val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
        filters, "UTC")
      hms_partitions.foreach { partition =>
        val hms_files = hms_util.hms_ext.listFiles(partition)
      }
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructHMSFilters(partition_name, min_sk, partition_range)

      val start_time = Instant.now()
      val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
        filters, "UTC")
      hms_partitions.foreach { partition =>
        val hms_files = hms_util.hms_ext.listFiles(partition)
      }
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "hms", num_files)
  }

  private def constructTreeFilters(partition_name : String, min_sk: Int, offset: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val min_sk_str = "%012d".format(min_sk)
    val max_sk_str = "%012d".format(min_sk + offset)
    val partition_pred = f"$partition_name >= '$partition_name=$min_sk_str' and " +
      f"$partition_name < '$partition_name=$max_sk_str'"
    filters.append(sql_parser.parseExpression(partition_pred))

    filters
  }

  private def scanTree(result_output : String, iters : Int, db_name : String,
                       table_name : String, partition_name : String, num_files : Int,
                       partition_range : Int, min_sks : Seq[Int], tree_address : String) : Unit = {

    val tree_util = new TreeUtil(tree_address)
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructTreeFilters(partition_name, min_sk, partition_range)
      val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructTreeFilters(partition_name, min_sk, partition_range)

      val start_time = Instant.now()
      val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "tree", num_files)
  }

  private def constructIcebergFilters(partition_name : String, min_sk: Int, offset: Int) :
  org.apache.iceberg.expressions.Expression = {
    val partition_pred = f"${partition_name} >= $min_sk and " +
      f"${partition_name} < ${min_sk + offset}"

    SparkExpressionConverter
      .convertToIcebergExpression(sql_parser.parseExpression(partition_pred))
  }

  private def scanIceberg(result_output : String, iters : Int, db_name : String,
                          table_name : String, partition_name : String, num_files : Int,
                          partition_range : Int, min_sks : Seq[Int]) : Unit = {

    val iceberg_util = new IcebergUtil()
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructIcebergFilters(partition_name, min_sk, partition_range)

      val iceberg_table = iceberg_util.iceberg
        .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
      val iceberg_plan_files = iceberg_table.table().newScan().filter(filters).planFiles()
      iceberg_plan_files.forEach { plan_file =>
        val file = plan_file.file()
      }
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructIcebergFilters(partition_name, min_sk, partition_range)

      val start_time = Instant.now()
      val iceberg_table = iceberg_util.iceberg
        .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
      val iceberg_plan_files = iceberg_table.table().newScan().filter(filters).planFiles()
      iceberg_plan_files.forEach { plan_file =>
        val file = plan_file.file()
      }
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "iceberg", num_files)
  }

}

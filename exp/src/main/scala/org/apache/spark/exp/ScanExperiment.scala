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

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.iceberg.spark.source.SparkTable

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

object ScanExperiment {
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("resultOutput", "/tmp/scanexperiment.json")
  misc_config.put("experimentIters", "5")
  misc_config.put("treeAddress", "localhost:9876")

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

    val catalog_type = args(1)
    val num_files = args(2).toInt

    catalog_type match {
      case "delta" => scanDelta(result_output, iters, delta_db, table_name, num_files)
      case "hms" => scanHMS(result_output, iters, hms_db, table_name, num_files)
      case "iceberg" => scanIceberg(result_output, iters, iceberg_db, table_name, num_files)
      case "tree" => scanTree(result_output, iters, tree_db, table_name, num_files, tree_address)
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

  private def scanDelta(result_output : String, iters : Int, db_name : String,
                        table_name : String, num_files : Int) : Unit = {

    val delta_util = new DeltaUtil()
    // dry run
    for (i <- 1 to 2) {
      val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
      val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(Seq.empty, Seq.empty)
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
      val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(Seq.empty, Seq.empty)
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "delta", num_files)
  }

  private def scanHMS(result_output : String, iters : Int, db_name : String,
                        table_name : String, num_files : Int) : Unit = {

    val hms_util = new HMSUtil()
    // dry run
    for (i <- 1 to 2) {
      val hmsPartitions = hms_util.hms.listPartitions(db_name, table_name)
      hmsPartitions.foreach { partition =>
        val hmsFiles = hms_util.hms_ext.listFiles(partition)
      }
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      val hmsPartitions = hms_util.hms.listPartitions(db_name, table_name)
      hmsPartitions.foreach { partition =>
        val hmsFiles = hms_util.hms_ext.listFiles(partition)
      }
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "hms", num_files)
  }

  private def scanTree(result_output : String, iters : Int, db_name : String,
                      table_name : String, num_files : Int, tree_address : String) : Unit = {

    val tree_util = new TreeUtil(tree_address)
    // dry run
    for (i <- 1 to 2) {
      val tree_files = tree_util.tree.listFiles(db_name, table_name, None)
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      val tree_files = tree_util.tree.listFiles(db_name, table_name, None)
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "tree", num_files)
  }

  private def scanIceberg(result_output : String, iters : Int, db_name : String,
                       table_name : String, num_files : Int) : Unit = {

    val iceberg_util = new IcebergUtil()
    // dry run
    for (i <- 1 to 2) {
      val iceberg_table = iceberg_util.iceberg
        .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
      val iceberg_plan_files = iceberg_table.table().newScan().planFiles()
      iceberg_plan_files.forEach { plan_file =>
        plan_file.file()
      }
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      val iceberg_table = iceberg_util.iceberg
        .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
      val iceberg_plan_files = iceberg_table.table().newScan().planFiles()
      iceberg_plan_files.forEach { plan_file =>
        plan_file.file()
      }
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "iceberg", num_files)
  }

}

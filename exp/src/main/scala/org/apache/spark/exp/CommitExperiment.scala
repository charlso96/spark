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
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.iceberg.spark.SparkCatalog

import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier}
import org.apache.spark.sql.connector.catalog.TableChange.renameColumn
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.tree.grpc.Grpccatalog


object CommitExperiment {
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("summaryOutput", "/tmp/commit-summary.json")
  misc_config.put("latencyOutput", "/tmp/commit-latency.json")
  misc_config.put("experimentIters", "500")
  misc_config.put("experimentTime", "00:01:00")
  misc_config.put("treeAddress", "localhost:9876")

  private def convertToMilliseconds(time : String): Int = {
    val parts = time.split(":").map(_.toInt)
    val hours = parts(0)
    val minutes = parts(1)
    val seconds = parts(2)
    (hours * 3600 + minutes * 60 + seconds) * 1000
  }

  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.CommitExperiment " +
        "<scanConfig> <catalogType>\n")
      return
    }

    val json_parser = new ObjectMapper
    // read in data config
    val scan_config_json = json_parser.readTree(Source.fromFile(args(0)).mkString)
    scan_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val summary_output = misc_config("summaryOutput")
    val latency_output = misc_config("latencyOutput")

    val iters = misc_config("experimentIters").toInt
    val experiment_time = convertToMilliseconds(misc_config("experimentTime"))

    val tree_address = misc_config("treeAddress")

    val delta_db = scan_config_json.get("delta").asText()
    val hms_db = scan_config_json.get("hms").asText()
    val tree_db = scan_config_json.get("tree").asText()
    val iceberg_db = scan_config_json.get("iceberg").asText()
    val table_name = scan_config_json.get("tables").get(0).get("name").asText()
    val col_name = scan_config_json.get("tables").get(0).get("schema").get(0).get("name").asText()
    val col_name2 = col_name + "2"
    val col_names = Array(col_name, col_name2)

    val catalog_type = args(1)

    val thread_pool = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

    catalog_type match {
      case "delta" => testDelta(summary_output, latency_output, iters : Int,
                              experiment_time, delta_db, table_name,
                              col_names, thread_pool)
      case "hms" => testHMS(summary_output, latency_output, iters : Int,
                                    experiment_time, hms_db, table_name,
                                    col_names, thread_pool)
      case "iceberg" => testIceberg(summary_output, latency_output, iters : Int,
                                    experiment_time, iceberg_db, table_name,
                                    col_names, thread_pool)
      case "tree" => testTree(summary_output, latency_output, iters : Int,
                              experiment_time, tree_db, table_name,
                              col_names, thread_pool, tree_address)
      case _ => print("Invalid Catalog Type!!!")
    }

    thread_pool.shutdown()
  }

  private def writeOutput(summaryOutput : String, latencyOutput : String,
                          throughput : Int, times : Seq[Long], catalog : String,
                          experiment_time : Int): Unit = {
    val latencyOutputWriter = new FileWriter(new File(latencyOutput), true)
    times.foreach { time =>
      latencyOutputWriter.write("{\"catalog\":\"" + catalog + "\", ")
      latencyOutputWriter.write("\"time\":" + time + "}")
      latencyOutputWriter.write("\n")
    }
    latencyOutputWriter.flush()
    latencyOutputWriter.close()

    val summaryOutputWriter = new FileWriter(new File(summaryOutput), true)
    summaryOutputWriter.write("{\"catalog\":\"" + catalog + "\", ")
    summaryOutputWriter.write("\"experimentTime\":" + experiment_time / 1000 + ", ")
    summaryOutputWriter.write("\"throughput\":" + throughput + "}")
    summaryOutputWriter.write("\n")
    summaryOutputWriter.flush()
    summaryOutputWriter.close()
  }

  private def alterDelta(delta : DelegatingCatalogExtension, ident : Identifier,
                         col_names : Array[String], changed : Boolean) : Boolean = {
    if (changed) {
      delta.alterTable(ident, renameColumn(Array(col_names(1)), col_names(0)))
      false
    }
    else {
      delta.alterTable(ident, renameColumn(Array(col_names(0)), col_names(1)))
      true
    }
  }

  private def testDelta(summaryOutput : String, latencyOutput : String, iters : Int,
                        experiment_time : Int, db_name : String, table_name : String,
                        col_names : Array[String], thread_pool : ExecutionContext) : Unit = {

    val delta_util = new DeltaUtil()
    val ident = Identifier.of(Array(db_name), table_name)
    val changed = new AtomicBoolean(false)
    val exec_throughput = new AtomicBoolean(false)

    // dry run
    for (i <- 0 until 10) {
      changed.set(alterDelta(delta_util.delta, ident, col_names, changed.get()))
    }

    // measure throughput
    exec_throughput.set(true)
    val future = Future[Int] {
      var throughput = 0
      while (exec_throughput.get()) {
        changed.set(alterDelta(delta_util.delta, ident, col_names, changed.get()))
        throughput += 1
      }
      throughput
    }(thread_pool)

    Thread.sleep(experiment_time)
    exec_throughput.set(false)

    // scalastyle:off awaitresult
    val throughput = Await.result(future, scala.concurrent.duration.Duration.Inf)
    // scalastyle:on awaitresult

    // measure latency
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      changed.set(alterDelta(delta_util.delta, ident, col_names, changed.get()))
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(summaryOutput, latencyOutput, throughput, times, "delta", experiment_time)
  }

  private def alterHMS(hms : HiveExternalCatalog, db_name : String, table_name : String,
                       col_names : Array[String], changed : Boolean) : Boolean = {

    val old_col_name = if (changed) {
      col_names(1)
    }
    else {
      col_names(0)
    }

    val new_col_name = if (changed) {
      col_names(0)
    }
    else {
      col_names(1)
    }

    val cur_table = hms.getTable(db_name, table_name)
    val new_schema = ArrayBuffer[StructField]()
    cur_table.schema.fields.foreach { struct_field =>
      if (old_col_name == struct_field.name) {
        new_schema += struct_field.copy(name = new_col_name)
      }
      else {
        new_schema += struct_field
      }
    }
    val new_table = cur_table.copy(
      schema = StructType(new_schema))
    hms.client.alterTable(new_table)

    !changed
  }

  private def testHMS(summaryOutput : String, latencyOutput : String, iters : Int,
                        experiment_time : Int, db_name : String, table_name : String,
                        col_names : Array[String], thread_pool : ExecutionContext) : Unit = {
    val hms_util = new HMSUtil()
    val changed = new AtomicBoolean(false)
    val exec_throughput = new AtomicBoolean(false)

    // dry run
    for (i <- 0 until 10) {
      changed.set(alterHMS(hms_util.hms, db_name, table_name, col_names, changed.get()))
    }

    // measure throughput
    exec_throughput.set(true)
    val future = Future[Int] {
      var throughput = 0
      while (exec_throughput.get()) {
        changed.set(alterHMS(hms_util.hms, db_name, table_name, col_names, changed.get()))
        throughput += 1
      }
      throughput
    }(thread_pool)

    exec_throughput.set(true)
    Thread.sleep(experiment_time)
    exec_throughput.set(false)

    // scalastyle:off awaitresult
    val throughput = Await.result(future, scala.concurrent.duration.Duration.Inf)
    // scalastyle:on awaitresult

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      changed.set(alterHMS(hms_util.hms, db_name, table_name, col_names, changed.get()))
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(summaryOutput, latencyOutput, throughput, times, "hms", experiment_time)
  }

  private def alterTree(tree : TreeExternalCatalog, db_name : String, table_name : String,
                        col_names : Array[String], changed : Boolean) : Boolean = {

    val old_col_name = if (changed) {
      col_names(1)
    }
    else {
      col_names(0)
    }

    val new_col_name = if (changed) {
      col_names(0)
    }
    else {
      col_names(1)
    }

    val txn = tree.startTransaction(Grpccatalog.TxnMode.TXN_MODE_READ_WRITE)
    val cur_table = tree.getTable(db_name, table_name, txn)
    val new_schema = ArrayBuffer[StructField]()
    cur_table.schema.fields.foreach { struct_field =>
      if (old_col_name == struct_field.name) {
        new_schema += struct_field.copy(name = new_col_name)
      }
      else {
        new_schema += struct_field
      }
    }
    val new_table = cur_table.copy(schema = StructType(new_schema))
    tree.alterTable(new_table, txn)
    tree.commit(txn.get)

    !changed
  }

  private def testTree(summaryOutput : String, latencyOutput : String, iters : Int,
                      experiment_time : Int, db_name : String, table_name : String,
                      col_names : Array[String], thread_pool : ExecutionContext,
                       tree_address : String) : Unit = {
    val tree_util = new TreeUtil(tree_address)
    val changed = new AtomicBoolean(false)
    val exec_throughput = new AtomicBoolean(false)

    // dry run
    for (i <- 0 until 10) {
      changed.set(alterTree(tree_util.tree, db_name, table_name, col_names, changed.get()))
    }

    // measure throughput
    exec_throughput.set(true)
    val future = Future[Int] {
      var throughput = 0
      while (exec_throughput.get()) {
        changed.set(alterTree(tree_util.tree, db_name, table_name, col_names, changed.get()))
        throughput += 1
      }
      throughput
    }(thread_pool)

    exec_throughput.set(true)
    Thread.sleep(experiment_time)
    exec_throughput.set(false)

    // scalastyle:off awaitresult
    val throughput = Await.result(future, scala.concurrent.duration.Duration.Inf)
    // scalastyle:on awaitresult

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      changed.set(alterTree(tree_util.tree, db_name, table_name, col_names, changed.get()))
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(summaryOutput, latencyOutput, throughput, times, "tree", experiment_time)
  }


  private def alterIceberg(iceberg : SparkCatalog, ident : Identifier,
                           col_names : Array[String], changed : Boolean) : Boolean = {
    if (changed) {
      iceberg.alterTable(ident, renameColumn(Array(col_names(1)), col_names(0)))
      false
    }
    else {
      iceberg.alterTable(ident, renameColumn(Array(col_names(0)), col_names(1)))
      true
    }
  }

  private def testIceberg(summaryOutput : String, latencyOutput : String, iters : Int,
                        experiment_time : Int, db_name : String, table_name : String,
                        col_names : Array[String], thread_pool : ExecutionContext) : Unit = {

    val iceberg_util = new IcebergUtil()
    val ident = Identifier.of(Array(db_name), table_name)
    val changed = new AtomicBoolean(false)
    val exec_throughput = new AtomicBoolean(false)

    // dry run
    for (i <- 0 until 10) {
      changed.set(alterIceberg(iceberg_util.iceberg, ident, col_names, changed.get()))
    }

    // measure throughput
    exec_throughput.set(true)
    val future = Future[Int] {
      var throughput = 0
      while (exec_throughput.get()) {
        changed.set(alterIceberg(iceberg_util.iceberg, ident, col_names, changed.get()))
        throughput += 1
      }
      throughput
    }(thread_pool)

    Thread.sleep(experiment_time)
    exec_throughput.set(false)

    // scalastyle:off awaitresult
    val throughput = Await.result(future, scala.concurrent.duration.Duration.Inf)
    // scalastyle:on awaitresult

    // measure latency
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val start_time = Instant.now()
      changed.set(alterIceberg(iceberg_util.iceberg, ident, col_names, changed.get()))
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(summaryOutput, latencyOutput, throughput, times, "iceberg", experiment_time)
  }

}

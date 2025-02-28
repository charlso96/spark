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
import java.util.concurrent.Executors

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.iceberg.spark.source.SparkTable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, SparkExpressionConverter}

object ScanRangeExperiment {
  private val sql_parser = new ThreadLocal[SparkSqlParser]() {
    override def initialValue(): SparkSqlParser = new SparkSqlParser()
  }

  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("resultOutput", "/tmp/scanrange.json")
  misc_config.put("experimentIters", "100")
  misc_config.put("treeAddress", "localhost:9876")
  misc_config.put("startDate", "1998-01-01")
  misc_config.put("endDate", "2003-12-31")
  misc_config.put("partitionRange", "7")
  misc_config.put("seed", "0")
  misc_config.put("numCores", Runtime.getRuntime.availableProcessors().toString)
  misc_config.put("numFiles", "50")

  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.ParallelScanRangeExperiment " +
        "<scanConfig> <catalogType>\n")
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
    val num_files = misc_config("numFiles").toInt

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
    val num_cores = misc_config("numCores").toInt
    val thread_pool = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(num_cores))

    val random = new scala.util.Random(misc_config("seed").toLong)
    // generate min sks.
    val min_sks = ArrayBuffer[Int]()
    for (i <- 0 until (iters + 10)) {
      min_sks += random.nextInt(num_partitions - partition_range)
    }

    catalog_type match {
      case "delta" => scanDelta(result_output, iters, delta_db, table_name, partition_name,
        num_files, partition_range, min_sks, num_cores)
      case "hms" => scanHMS(result_output, iters, hms_db, table_name, partition_name, num_files,
        partition_range, min_sks, num_cores)
      case "phms" => scanParallelHMS(result_output, iters, hms_db, table_name, partition_name,
        num_files, partition_range, min_sks, num_cores, thread_pool)
      case "iceberg" => scanIceberg(result_output, iters, iceberg_db, table_name, partition_name,
        num_files, partition_range, min_sks, num_cores)
      case "tree" => scanTree(result_output, iters, tree_db, table_name, partition_name,
        num_files, partition_range, min_sks, tree_address, num_cores)
      case "ptree" => scanParallelTree(result_output, iters, (tree_db, table_name), partition_name,
        num_files, partition_range, min_sks, tree_address, num_cores, thread_pool)
      case _ => print("Invalid Catalog Type!!!")
    }

    thread_pool.shutdown()
  }

  private def writeOutput(result_output : String, times : Seq[Long], catalog : String,
                          num_files : Int, partition_range : Int, num_cores : Int): Unit = {
    val output_writer = new FileWriter(new File(result_output), true)
    times.foreach { time =>
      output_writer.write("{\"catalog\":\"" + catalog + "\", ")
      output_writer.write("\"numFiles\":" + num_files + ", ")
      output_writer.write("\"partitionRange\":" + partition_range + ", ")
      output_writer.write("\"numCores\":" + num_cores + ", ")
      output_writer.write("\"time\":" + time + "}")
      output_writer.write("\n")
    }
    output_writer.flush()
    output_writer.close()
  }

  def splitIntoBoundaries(min: Int, max: Int, n: Int): List[(Int, Int)] = {
    require(n > 0, "Number of boundaries must be positive")

    val step = (max - min) / n
    val remainder = (max - min) % n

    (0 until n).foldLeft((min, List.empty[(Int, Int)])) {
      case ((start, acc), i) =>
        val extra = if (i < remainder) 1 else 0 // Distribute remainder evenly
        val end = start + step + extra
        (end, acc :+ (start, end))
    }._2
  }


  private def constructHMSFilters(partition_name : String, min_sk: Int, max_sk: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val partition_pred = f"${partition_name} >= $min_sk and " +
      f"${partition_name} < $max_sk"
    filters.append(sql_parser.get.parseExpression(partition_pred))

    filters
  }

  private def scanDelta(result_output : String, iters : Int, db_name : String,
                        table_name : String, partition_name : String, num_files : Int,
                        partition_range : Int, min_sks : Seq[Int], num_cores : Int) : Unit = {

    val delta_util = new DeltaUtil()
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructHMSFilters(partition_name, min_sk, min_sk + partition_range)
      val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
      val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(filters, Seq.empty)
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructHMSFilters(partition_name, min_sk, min_sk + partition_range)

      val start_time = Instant.now()
      val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
      val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
        .asInstanceOf[HadoopFsRelation]
      val deltaPartitions = baseRelation.location.listFiles(filters, Seq.empty)
      val end_time = Instant.now()

      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "delta", num_files, partition_range, num_cores)
  }

  private def scanHMS(result_output : String, iters : Int, db_name : String,
                      table_name : String, partition_name : String, num_files : Int,
                      partition_range : Int, min_sks : Seq[Int], num_cores : Int) : Unit = {

    val hms_util = new HMSUtil()
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructHMSFilters(partition_name, min_sk, min_sk + partition_range)

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
      val filters = constructHMSFilters(partition_name, min_sk, min_sk + partition_range)

      val start_time = Instant.now()
      val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
        filters, "UTC")
      hms_partitions.foreach { partition =>
        val hms_files = hms_util.hms_ext.listFiles(partition)
      }
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "hms", num_files, partition_range, num_cores)
  }

  private def scanParallelHMS(result_output : String, iters : Int, db_name : String,
                      table_name : String, partition_name : String, num_files : Int,
                      partition_range : Int, min_sks : Seq[Int], num_cores : Int,
                      thread_pool : ExecutionContext) : Unit = {

    val hms_util = new HMSUtil()

    val table = hms_util.hms_ext.getTable(db_name, table_name)

    // Initialize multiple instances of hdfs clients because apparently they are not thread safe...
    val file_systems = ArrayBuffer[FileSystem]()
    for (i <- 0 until num_cores) {
      file_systems.append(FileSystem.newInstance(table.location, hms_util.hms_ext.hadoopConf))
    }

    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructHMSFilters(partition_name, min_sk, min_sk + partition_range)
      val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
        filters, "UTC")
      val num_tasks = num_cores.min(hms_partitions.length)
      // split partitions into groups
      val partition_groups = splitIntoBoundaries(0, hms_partitions.length, num_tasks)
      // assign listfiles operation on each group of partitions to a single task
      val tasks = (0 until num_tasks).map(j => Future {
          for (k <- partition_groups(j)._1 until partition_groups(j)._2 ) {
            val hms_files = file_systems(j).listStatus(new Path(hms_partitions(k).location)).toSeq
          }
      }(thread_pool))

      // scalastyle:off awaitresult
      Await.result(Future.sequence(tasks)(implicitly, thread_pool),
        scala.concurrent.duration.Duration.Inf)
      // scalastyle:on awaitresult
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructHMSFilters(partition_name, min_sk, min_sk + partition_range)
      val start_time = Instant.now()
      val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
        filters, "UTC")
      val num_tasks = num_cores.min(hms_partitions.length)
      // split partitions into groups
      val partition_groups = splitIntoBoundaries(0, hms_partitions.length, num_tasks)
      // assign listfiles operation of each partition group to a single task
      val tasks = (0 until num_tasks).map(j => Future {
        for (k <- partition_groups(j)._1 until partition_groups(j)._2 ) {
          val hms_files = file_systems(j).listStatus(new Path(hms_partitions(k).location)).toSeq
        }
      }(thread_pool))

      // scalastyle:off awaitresult
      Await.result(Future.sequence(tasks)(implicitly, thread_pool),
        scala.concurrent.duration.Duration.Inf)
      // scalastyle:on awaitresult
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "phms", num_files, partition_range, num_cores)
  }

  private def constructTreeFilters(partition_name : String, min_sk: Int, max_sk: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val min_sk_str = "%012d".format(min_sk)
    val max_sk_str = "%012d".format(max_sk)
    val partition_pred = f"$partition_name >= '$partition_name=$min_sk_str' and " +
      f"$partition_name < '$partition_name=$max_sk_str'"
    filters.append(sql_parser.get.parseExpression(partition_pred))

    filters
  }

  private def scanTree(result_output : String, iters : Int, db_name : String,
                       table_name : String, partition_name : String, num_files : Int,
                       partition_range : Int, min_sks : Seq[Int], tree_address : String,
                       num_cores : Int) : Unit = {

    val tree_util = new TreeUtil(tree_address)
    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      val filters = constructTreeFilters(partition_name, min_sk, min_sk + partition_range)
      val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val filters = constructTreeFilters(partition_name, min_sk, min_sk + partition_range)

      val start_time = Instant.now()
      val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "tree", num_files, partition_range, num_cores)
  }

  private def scanParallelTree(result_output : String, iters: Int, db_table_names: (String, String),
                       partition_name : String, num_files : Int,
                       partition_range : Int, min_sks : Seq[Int], tree_address : String,
                       num_cores : Int, thread_pool : ExecutionContext) : Unit = {

    val db_name = db_table_names._1
    val table_name = db_table_names._2

    val tree_util = new TreeUtil(tree_address)

    // In case there are less cores than 10
    val range_groups = splitIntoBoundaries(0, 10, num_cores)
    val predicates = range_groups.map{ boundary =>
      val pred_list = ArrayBuffer[String]()
      for (j <- boundary._1 until boundary._2) {
        pred_list += f"endswith($partition_name, '$j')"
      }
      pred_list.mkString(" or ")
    }
    val num_tasks = num_cores.min(10)

    // dry run
    for (i <- 0 until 10) {
      val min_sk = min_sks(i)
      // assign each range group to a single task
      val tasks = (0 until num_tasks).map(j => Future {
        val filters = ArrayBuffer[Expression]()
        val min_sk_str = "%012d".format(min_sk)
        val max_sk_str = "%012d".format(min_sk + partition_range)
        val partition_pred = f"$partition_name >= '$partition_name=$min_sk_str' and " +
          f"$partition_name < '$partition_name=$max_sk_str' and (${predicates(j)})"
        filters.append(sql_parser.get.parseExpression(partition_pred))
        val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
      }(thread_pool))
      // scalastyle:off awaitresult
      Await.result(Future.sequence(tasks)(implicitly, thread_pool),
        scala.concurrent.duration.Duration.Inf)
      // scalastyle:on awaitresult

    }

    // actual experiment
    val times = ArrayBuffer[Long]()
    for (i <- 0 until iters) {
      val min_sk = min_sks(i + 10)
      val start_time = Instant.now()
      // assign each range group to a single task
      val tasks = (0 until num_tasks).map(j => Future {
        val filters = ArrayBuffer[Expression]()
        val min_sk_str = "%012d".format(min_sk)
        val max_sk_str = "%012d".format(min_sk + partition_range)
        val partition_pred = f"$partition_name >= '$partition_name=$min_sk_str' and " +
          f"$partition_name < '$partition_name=$max_sk_str' and (${predicates(j)})"
        filters.append(sql_parser.get.parseExpression(partition_pred))
        val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
      }(thread_pool))
      // scalastyle:off awaitresult
      Await.result(Future.sequence(tasks)(implicitly, thread_pool),
        scala.concurrent.duration.Duration.Inf)
      // scalastyle:on awaitresult
      val end_time = Instant.now()
      times += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "ptree", num_files, partition_range, num_cores)
  }

  private def constructIcebergFilters(partition_name : String, min_sk: Int, offset: Int) :
  org.apache.iceberg.expressions.Expression = {
    val partition_pred = f"${partition_name} >= $min_sk and " +
      f"${partition_name} < ${min_sk + offset}"

    SparkExpressionConverter
      .convertToIcebergExpression(sql_parser.get.parseExpression(partition_pred))
  }

  private def scanIceberg(result_output : String, iters : Int, db_name : String,
                          table_name : String, partition_name : String, num_files : Int,
                          partition_range : Int, min_sks : Seq[Int], num_cores : Int) : Unit = {

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

    writeOutput(result_output, times, "iceberg", num_files, partition_range, num_cores)
  }

}

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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.iceberg.spark.source.SparkTable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, SparkExpressionConverter}



object TPCDSExperiment {
  private val sql_parser = new SparkSqlParser()
  val misc_config = scala.collection.mutable.Map.empty[String, String]
  misc_config.put("resultOutput", "/tmp/dsexperiment.json")
  misc_config.put("experimentIters", "10")
  misc_config.put("scaleFactor", "100T")
  misc_config.put("treeAddress", "localhost:9876")
  misc_config.put("startDate", "1998-01-01")
  misc_config.put("endDate", "2003-12-31")
  misc_config.put("seed", "0")

  def main(args: Array[String]): Unit = {
    if (args.size != 3) {
      print("Usage: spark-class org.apache.spark.exp.TPCDSExperiment " +
        "<dataConfig> <workloadConfig> <catalogType>\n")
      return
    }

    val catalog_type = args(2)
    val json_parser = new ObjectMapper
    // read in data config
    val data_config_json = json_parser.readTree(Source.fromFile(args(0)).mkString)
    // read in workload config
    val workload_config_json = json_parser.readTree(Source.fromFile(args(1)).mkString)
    // initialize misc config map
    workload_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val result_output = misc_config("resultOutput")
    val iters = misc_config("experimentIters").toInt
    val tree_address = misc_config("treeAddress")

    val delta_db = data_config_json.get("delta").asText()
    val hms_db = data_config_json.get("hms").asText()
    val tree_db = data_config_json.get("tree").asText()
    val iceberg_db = data_config_json.get("iceberg").asText()

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

    val random = new scala.util.Random(misc_config("seed").toLong)
    catalog_type match {
      case "delta" => testDelta(result_output, iters, delta_db, read_config, table_configs, random)
      case "hms" => testHMS(result_output, iters, hms_db, read_config, table_configs, random)
      case "iceberg" => testIceberg(result_output, iters, iceberg_db, read_config, table_configs,
        random)
      case "tree" => testTree(result_output, iters, tree_db, read_config, table_configs, random,
        tree_address)
      case _ => print("Invalid Catalog Type!!!")
    }
  }

  private def writeOutput(result_output : String, times : mutable.HashMap[Int, ArrayBuffer[Long]],
                          catalog : String): Unit = {
    val output_writer = new FileWriter(new File(result_output), true)
    times.foreach { time_list =>
      val query_name = "query" + (time_list._1 + 1)
      time_list._2.foreach { time =>
        output_writer.write("{\"catalog\":\"" + catalog + "\", ")
        output_writer.write("\"query\":\"" + query_name + "\", ")
        output_writer.write("\"time\":" + time + "}")
        output_writer.write("\n")
      }
    }
    output_writer.flush()
    output_writer.close()
  }

  private def constructHMSFilters(partition_name : String, sk: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val partition_pred = f"${partition_name} = $sk"
    filters.append(sql_parser.parseExpression(partition_pred))

    filters
  }

  private def constructHMSFilters(partition_name : String, min_sk: Int, max_sk: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val partition_pred = f"${partition_name} >= $min_sk and " +
      f"${partition_name} <= $max_sk"
    filters.append(sql_parser.parseExpression(partition_pred))

    filters
  }

  private def testDelta(result_output : String, iters : Int, db_name : String,
                        read_config : ReadConfig,
                        table_configs : mutable.HashMap[String, TableConfig],
                        random : scala.util.Random) : Unit = {

    val delta_util = new DeltaUtil()

    // dry run
    for (i <- 0 until (iters / 10)) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
          val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
            .asInstanceOf[HadoopFsRelation]
          val deltaPartitions = baseRelation.location.listFiles(Seq.empty, Seq.empty)

        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name
          val partition_range = table_json.get("range")

          val filters = if (partition_range.size() == 1) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt())
          }
          else if (partition_range.size() == 2) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt(),
              partition_range.get(1).asInt())
          }
          else {
            ArrayBuffer[Expression]()
          }

          val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
          val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
            .asInstanceOf[HadoopFsRelation]
          val deltaPartitions = baseRelation.location.listFiles(filters, Seq.empty)

        }

      }

    }

    // actual experiment
    val times = mutable.HashMap[Int, ArrayBuffer[Long]]()
    for (i <- 0 until iters) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()

      val start_time = Instant.now()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
          val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
            .asInstanceOf[HadoopFsRelation]
          val deltaPartitions = baseRelation.location.listFiles(Seq.empty, Seq.empty)
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name

          val partition_range = table_json.get("range")

          val filters = if (partition_range.size() == 1) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt())
          }
          else if (partition_range.size() == 2) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt(),
              partition_range.get(1).asInt())
          }
          else {
            ArrayBuffer[Expression]()
          }

          val deltaTable = delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
          val baseRelation = deltaTable.asInstanceOf[DeltaTableV2].toBaseRelation
            .asInstanceOf[HadoopFsRelation]
          val deltaPartitions = baseRelation.location.listFiles(filters, Seq.empty)
        }

      }
      val end_time = Instant.now()

      if (!times.contains(query_idx)) {
        times.put(query_idx, new ArrayBuffer[Long])
      }

      times(query_idx) += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "delta")
  }

  private def testHMS(result_output : String, iters : Int, db_name : String,
                        read_config : ReadConfig,
                        table_configs : mutable.HashMap[String, TableConfig],
                        random : scala.util.Random) : Unit = {

    val hms_util = new HMSUtil()

    // dry run
    for (i <- 0 until (iters / 10)) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val table = hms_util.hms.getTable(db_name, table_name)
          val hmsFiles = hms_util.hms_ext.listFiles(table)
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name
          val partition_range = table_json.get("range")

          val filters = if (partition_range.size() == 1) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt())
          }
          else if (partition_range.size() == 2) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt(),
              partition_range.get(1).asInt())
          }
          else {
            ArrayBuffer[Expression]()
          }

          val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
            filters, "UTC")
          hms_partitions.foreach { partition =>
            val hms_files = hms_util.hms_ext.listFiles(partition)
          }

        }

      }

    }

    // actual experiment
    val times = mutable.HashMap[Int, ArrayBuffer[Long]]()
    for (i <- 0 until iters) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()

      val start_time = Instant.now()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val table = hms_util.hms.getTable(db_name, table_name)
          val hmsFiles = hms_util.hms_ext.listFiles(table)
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name

          val partition_range = table_json.get("range")

          val filters = if (partition_range.size() == 1) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt())
          }
          else if (partition_range.size() == 2) {
            constructHMSFilters(partition_col_name, partition_range.get(0).asInt(),
              partition_range.get(1).asInt())
          }
          else {
            ArrayBuffer[Expression]()
          }

          val hms_partitions = hms_util.hms.listPartitionsByFilter(db_name, table_name,
            filters, "UTC")
          hms_partitions.foreach { partition =>
            val hms_files = hms_util.hms_ext.listFiles(partition)
          }

        }

      }
      val end_time = Instant.now()

      if (!times.contains(query_idx)) {
        times.put(query_idx, new ArrayBuffer[Long])
      }

      times(query_idx) += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "hms")
  }

  private def constructTreeFilters(partition_name : String, sk: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val sk_str = "%012d".format(sk)
    val partition_pred = f"$partition_name = '$partition_name=$sk_str'"
    filters.append(sql_parser.parseExpression(partition_pred))

    filters
  }

  private def constructTreeFilters(partition_name : String, min_sk: Int, max_sk: Int) :
  Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()
    val min_sk_str = "%012d".format(min_sk)
    val max_sk_str = "%012d".format(max_sk)
    val partition_pred = f"$partition_name >= '$partition_name=$min_sk_str' and " +
      f"$partition_name <= '$partition_name=$max_sk_str'"
    filters.append(sql_parser.parseExpression(partition_pred))

    filters
  }

  private def testTree(result_output : String, iters : Int, db_name : String,
                      read_config : ReadConfig,
                      table_configs : mutable.HashMap[String, TableConfig],
                      random : scala.util.Random, tree_address : String) : Unit = {

    val tree_util = new TreeUtil(tree_address)

    // dry run
    for (i <- 0 until (iters / 10)) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val tree_files = tree_util.tree.listFiles(db_name, table_name, None)
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name
          val partition_range = table_json.get("range")

          val filters = if (partition_range.size() == 1) {
            constructTreeFilters(partition_col_name, partition_range.get(0).asInt())
          }
          else if (partition_range.size() == 2) {
            constructTreeFilters(partition_col_name, partition_range.get(0).asInt(),
              partition_range.get(1).asInt())
          }
          else {
            ArrayBuffer[Expression]()
          }

          val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)

        }

      }

    }

    // actual experiment
    val times = mutable.HashMap[Int, ArrayBuffer[Long]]()
    for (i <- 0 until iters) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()

      val start_time = Instant.now()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val tree_files = tree_util.tree.listFiles(db_name, table_name, None)
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name

          val partition_range = table_json.get("range")

          val filters = if (partition_range.size() == 1) {
            constructTreeFilters(partition_col_name, partition_range.get(0).asInt())
          }
          else if (partition_range.size() == 2) {
            constructTreeFilters(partition_col_name, partition_range.get(0).asInt(),
              partition_range.get(1).asInt())
          }
          else {
            ArrayBuffer[Expression]()
          }

          val tree_files = tree_util.tree.listFilesByFilter(db_name, table_name, filters, None)
        }

      }
      val end_time = Instant.now()

      if (!times.contains(query_idx)) {
        times.put(query_idx, new ArrayBuffer[Long])
      }

      times(query_idx) += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "tree")
  }

  private def constructIcebergFilters(partition_name : String, sk: Int) :
  org.apache.iceberg.expressions.Expression = {
    val partition_pred = f"${partition_name} = $sk"

    SparkExpressionConverter
      .convertToIcebergExpression(sql_parser.parseExpression(partition_pred))
  }

  private def constructIcebergFilters(partition_name : String, min_sk: Int, max_sk: Int) :
  org.apache.iceberg.expressions.Expression = {
    val partition_pred = f"${partition_name} >= $min_sk and " +
      f"${partition_name} <= ${max_sk}"

    SparkExpressionConverter
      .convertToIcebergExpression(sql_parser.parseExpression(partition_pred))
  }

  private def testIceberg(result_output : String, iters : Int, db_name : String,
                       read_config : ReadConfig,
                       table_configs : mutable.HashMap[String, TableConfig],
                       random : scala.util.Random) : Unit = {

    val iceberg_util = new IcebergUtil()

    // dry run
    for (i <- 0 until (iters / 10)) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val iceberg_table = iceberg_util.iceberg
            .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
          val iceberg_plan_files = iceberg_table.table().newScan().planFiles()
          iceberg_plan_files.forEach { plan_file =>
            val file = plan_file.file()
          }
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name
          val partition_range = table_json.get("range")

          if (partition_range.size() == 1) {
            val filters = constructIcebergFilters(partition_col_name, partition_range.get(0)
              .asInt())
            val iceberg_table = iceberg_util.iceberg
              .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
            val iceberg_plan_files = iceberg_table.table().newScan().filter(filters).planFiles()
            iceberg_plan_files.forEach { plan_file =>
              val file = plan_file.file()
            }
          }
          else if (partition_range.size() == 2) {
            val filters = constructIcebergFilters(partition_col_name, partition_range.get(0)
              .asInt(), partition_range.get(1).asInt())
            val iceberg_table = iceberg_util.iceberg
              .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
            val iceberg_plan_files = iceberg_table.table().newScan().filter(filters).planFiles()
            iceberg_plan_files.forEach { plan_file =>
              val file = plan_file.file()
            }
          }
          else {
            ArrayBuffer[Expression]()
            val iceberg_table = iceberg_util.iceberg
              .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
            val iceberg_plan_files = iceberg_table.table().newScan().planFiles()
            iceberg_plan_files.forEach { plan_file =>
              val file = plan_file.file()
            }
          }

        }

      }

    }

    // actual experiment
    val times = mutable.HashMap[Int, ArrayBuffer[Long]]()
    for (i <- 0 until iters) {
      val query_idx = random.nextInt(read_config.queries.length)
      val query_config = read_config.queries(query_idx)
      val table_jsons = query_config.elements().next()

      val start_time = Instant.now()
      table_jsons.forEach{ table_json =>
        // if json is simply a string, it specifies a dimension table
        if (table_json.getNodeType() == JsonNodeType.STRING) {
          val table_name = table_json.asText()
          val iceberg_table = iceberg_util.iceberg
            .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
          val iceberg_plan_files = iceberg_table.table().newScan().planFiles()
          iceberg_plan_files.forEach { plan_file =>
            val file = plan_file.file()
          }
        }
        // otherwise (json object), a partitioned fact table
        else {
          val table_name = table_json.get("name").asText()
          val partition_col_name = table_configs(table_name).partition_schema.head.name
          val partition_range = table_json.get("range")

          if (partition_range.size() == 1) {
            val filters = constructIcebergFilters(partition_col_name, partition_range.get(0)
              .asInt())
            val iceberg_table = iceberg_util.iceberg
              .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
            val iceberg_plan_files = iceberg_table.table().newScan().filter(filters).planFiles()
            iceberg_plan_files.forEach { plan_file =>
              val file = plan_file.file()
            }
          }
          else if (partition_range.size() == 2) {
            val filters = constructIcebergFilters(partition_col_name, partition_range.get(0)
              .asInt(), partition_range.get(1).asInt())
            val iceberg_table = iceberg_util.iceberg
              .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
            val iceberg_plan_files = iceberg_table.table().newScan().filter(filters).planFiles()
            iceberg_plan_files.forEach { plan_file =>
              val file = plan_file.file()
            }
          }
          else {
            ArrayBuffer[Expression]()
            val iceberg_table = iceberg_util.iceberg
              .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable]
            val iceberg_plan_files = iceberg_table.table().newScan().planFiles()
            iceberg_plan_files.forEach { plan_file =>
              val file = plan_file.file()
            }
          }

        }

      }
      val end_time = Instant.now()

      if (!times.contains(query_idx)) {
        times.put(query_idx, new ArrayBuffer[Long])
      }

      times(query_idx) += Duration.between(start_time, end_time).toNanos()
    }

    writeOutput(result_output, times, "iceberg")
  }

}
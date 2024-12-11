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

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.log
import scala.util.Random

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType

import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogStorageFormat, CatalogTableFile}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DateType, IntegralType}
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

class TableGenerator(fact_weights : Map[String, Int], dimension_weights : Map[String, Int]) {
  val cumulative_fact_weights: List[(String, Int)] = fact_weights.toList.scanLeft(("", 0)) {
    case ((_, cumulative), (string, weight)) => (string, cumulative + weight)
  }.tail

  private val total_fact_weight = cumulative_fact_weights.last._2

  val cumulative_dimension_weights: List[(String, Int)] = dimension_weights.toList.scanLeft(("", 0))
  {
    case ((_, cumulative), (string, weight)) => (string, cumulative + weight)
  }.tail

  private val total_dimension_weight = cumulative_dimension_weights.last._2

  def genFactTable() : String = {
    val random_val = Random.nextInt(total_fact_weight)
    cumulative_fact_weights.find {
      case (_, cumulative) => random_val < cumulative
    }.map(_._1).get
  }

  def genDimensionTable() : String = {
    val random_val = Random.nextInt(total_dimension_weight)
    cumulative_dimension_weights.find {
      case (_, cumulative) => random_val < cumulative
    }.map(_._1).get
  }

}


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

    val insert_config = new InsertConfig(workload_config_json.get("insert"))
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

    // Initialize the table generator. Probabilities are weighed by
    // log(num_rows) for now
    val fact_weights = scala.collection.mutable.Map[String, Int]()
    val dimension_weights = scala.collection.mutable.Map[String, Int]()
    table_configs.foreach { table_entry =>
      table_entry._2.table_type match {
        case TableType.Fact => fact_weights.put(table_entry._1, (log(table_entry._2.num_rows)/
          log(misc_config("tableGenBase").toDouble)).toInt)
        case TableType.Dimension => dimension_weights.put(table_entry._1,
          (log(table_entry._2.num_rows)/log(misc_config("tableGenBase").toDouble)).toInt)
      }
    }

    val tree_cat = new TreeExternalCatalog(misc_config("treeAddress"))
    val table_generator = new TableGenerator(fact_weights.toMap, dimension_weights.toMap)

    // start of insert operation
    val txn = tree_cat.startTransaction(TxnMode.TXN_MODE_READ_WRITE)
    // randomly choose a fact table, using the table generator
    val dest_fact_table_name = table_generator.genFactTable()
    // decide whether to insert to dimension tables
    val dest_dim_table_names = ArrayBuffer[String]()
    var add_dimension = true
    var idx = 0
    // there are 2 tables, customer and customer address for now
    while (add_dimension) {
      add_dimension = (Random.nextDouble() < insert_config.dimension_update_rates(idx))
      if (add_dimension) {
        dest_dim_table_names.append(insert_config.dim_tables(idx))
      }
      idx = idx + 1
    }

    val dest_fact_table_config = table_configs(dest_fact_table_name)
    // get the target fact table
    val dest_fact_table = tree_cat.getTable(database_name, dest_fact_table_config.name, txn)
    // column statistics
    val col_stats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
    dest_fact_table_config.schema.foreach { attr_config =>
      if (attr_config.key.isDefined) {
        val dim_table_config = table_configs(attr_config.key.get)
        val cardinality = dim_table_config.high_watermark.get.toInt
        var min_sk = Random.nextInt(cardinality)
        var max_sk = min_sk
        for (i <- 0 until (insert_config.insert_ratio * dest_fact_table_config.num_rows/10)
          .toInt) {
          val rand_sk = Random.nextInt(cardinality)
          if (rand_sk < min_sk ) {
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

          val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
            Some(BigInt(0)), None, None, None, 1)
          col_stats.put(attr_config.name, col_stat)
        }
        else {
          if (attr_config.data_type == "DATE") {
            // simply scale down min and max to the date range
            min_sk = min_sk * dates.length/cardinality
            max_sk = max_sk * dates.length/cardinality
            val col_stat = CatalogColumnStat(None, Some(dates(min_sk)), Some(dates(max_sk)),
              Some(BigInt(0)), None, None, None, 1)
            col_stats.put(attr_config.name, col_stat)
          }
          else {
            // just get all the files of the dimension table
            val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
            // get the list of files from the dimension table
            tree_cat.listFiles(table, txn)
            val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
              Some(BigInt(0)), None, None, None, 1)
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
            min = dates(Random.nextInt(dates.length))
            val temp = dates(Random.nextInt(dates.length))
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
        dest_part_spec.put(partition_col.name, Random.nextInt(dates.length).toString)
      }
      // partition col follows uniform distribution for now
      if (partition_col.dataType == DateType) {
        dest_part_spec.put(partition_col.name, dates(Random.nextInt(dates.length)))
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

    // finally add batch of files to the chosen partition
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
          var min_sk = Random.nextInt(cardinality)
          var max_sk = min_sk

          if (dest_dim_table_name == attr_config.key.get) {
            val num_new_dim_records = (insert_config.insert_ratio * dest_dim_table_config.num_rows)
              .toLong
            val min_sk = dest_dim_table_config.high_watermark.getAndAdd(num_new_dim_records)
            val max_sk = min_sk + num_new_dim_records - 1

          }
          else {
            for (i <- 0 until (insert_config.insert_ratio * dest_fact_table_config.num_rows/10)
              .toInt) {
              val rand_sk = Random.nextInt(cardinality)
              if (rand_sk < min_sk ) {
                min_sk = rand_sk
              }
              if (rand_sk > max_sk) {
                max_sk = rand_sk
              }
            }
          }

          if (dim_table_config.business_id.isDefined) {
            val filters = ArrayBuffer[Expression]()
            val min_id = skToId(16, min_sk)
            val max_id = skToId(16, max_sk)
            val pred = f"${dim_table_config.business_id.get.name} >= $min_id and " +
              f"${dim_table_config.business_id.get.name} <= $max_id"
            filters.append(sqlParser.parseExpression(pred))
            // get the dimension table
            val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
            // get the list of files from the dimension table
            tree_cat.listFilesByFilter(table, filters, txn)

            val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
              Some(BigInt(0)), None, None, None, 1)
            col_stats.put(attr_config.name, col_stat)

          }
          else {
            if (attr_config.data_type == "DATE") {
              // simply scale down min and max to the date range
              min_sk = min_sk * dates.length/cardinality
              max_sk = max_sk * dates.length/cardinality
              val col_stat = CatalogColumnStat(None, Some(dates(min_sk)), Some(dates(max_sk)),
                Some(BigInt(0)), None, None, None, 1)
              col_stats.put(attr_config.name, col_stat)
            }
            else {
              // just get all the files of the dimension table
              val table = tree_cat.getTable(database_name, dim_table_config.name, txn)
              // get the list of files from the dimension table
              tree_cat.listFiles(table, txn)
              val col_stat = CatalogColumnStat(None, Some(min_sk.toString), Some(max_sk.toString),
                Some(BigInt(0)), None, None, None, 1)
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
              min = dates(Random.nextInt(dates.length))
              val temp = dates(Random.nextInt(dates.length))
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

  def genRandomDecimal(num_digits : Long) : Int = {
    Random.nextInt(math.pow(10, num_digits - 4).toInt)
  }

  def genRandomVarChar(max_length : Long) : String = {
    val length = Random.nextInt(max_length.toInt)
    val char_buf = ArrayBuffer.fill(length)('A')
    for (i <- 0 until length) {
      char_buf(i) = (Random.nextInt(26) + 65).toChar
    }
    char_buf.mkString
  }

  def skToId(length : Int, sk : Int) : String = {
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
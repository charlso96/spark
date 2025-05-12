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

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.iceberg.DataFile
import org.apache.iceberg.Transaction
import org.apache.iceberg.spark.source.SparkTable
import org.json4s.{CustomSerializer, NoTypeHints}
import org.json4s.JsonAST.{JNull, JObject, JString}
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableFile}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.{DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.tree.TreeTxn
import org.apache.spark.tree.grpc.Grpccatalog


object EndToEndExperiment {
  def main(args: Array[String]): Unit = {
    if (args.size != 2) {
      print("Usage: spark-class org.apache.spark.exp.EndToEndExperiment " +
        "<configFile> <catalogType>\n")
      return
    }

    val impl = new EndToEndImpl(args(0), args(1))
    impl.run()

  }

  private class HiveURISerializer extends CustomSerializer[URI](format =>
    (
      {
        case JString(s) => URI.create(s)
        case JNull => null
      },
      { case x: URI =>
        JString(x.toString)
      }
    )
  )

  private class HiveDataTypeSerializer extends CustomSerializer[DataType](format =>
    (
      {
        case JObject(o) => DataType.parseDataType(JObject(o))
        case JNull => null
      },
      { case x: DataType =>
        x.jsonValue
      }
    )
  )

  private class HiveStructTypeSerializer extends CustomSerializer[StructType](format =>
    (
      {
        case JObject(o) => DataType.parseDataType(JObject(o)).asInstanceOf[StructType]
        case JNull => null
      },
      { case x: StructType =>
        x.jsonValue
      }
    )
  )

  private class HiveMetadataSerializer extends CustomSerializer[Metadata](format =>
    (
      {
        case JObject(o) => Metadata.fromJObject(JObject(o))
        case JNull => null
      },
      { case x: Metadata =>
        Metadata.toJsonValue(x)
      }
    )
  )

  private class EndToEndImpl(config : String, catalog_type_str : String) {
    private val misc_config = scala.collection.mutable.Map.empty[String, String]
    misc_config.put("summaryOutput", "/tmp/endtoend-summary.json")
    misc_config.put("latencyOutput", "/tmp/endtoend-latency.json")
    misc_config.put("execScript", "/tmp/execendtoend.py")
    misc_config.put("experimentIters", "500")
    misc_config.put("experimentTime", "00:05:00")
    misc_config.put("webHDFS", "localhost:9870")
    misc_config.put("treeAddress", "localhost:9876")
    misc_config.put("startDate", "1998-01-01")
    misc_config.put("endDate", "2003-12-31")

    val json_parser = new ObjectMapper
    // read in data config
    val scan_config_json = json_parser.readTree(Source.fromFile(config).mkString)
    scan_config_json.get("misc").fields().forEachRemaining { config =>
      misc_config.put(config.getKey, config.getValue.asText())
    }

    val tree_address = misc_config("treeAddress")

    private val catalog_type : Int = catalog_type_str match {
      case "delta" => 0
      case "iceberg" => 1
      case "tree" => 2
      case _ => 3
    }
    if (catalog_type == 3) {
      print("Invalid Catalog!!\n")
    }

    // initialize the dates
    val date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start_date = LocalDate.parse(misc_config("startDate"), date_formatter)
    val end_date = LocalDate.parse(misc_config("endDate"), date_formatter)
    val dates: Array[String] = Iterator.iterate(start_date)(_ plusDays 1) // Generate dates
      .takeWhile(!_.isAfter(end_date)) // Stop when exceeding the end date
      .map(_.format(date_formatter)) // Convert to formatted strings
      .toArray

    private implicit val formats = Serialization.formats(NoTypeHints) + new HiveURISerializer +
      new HiveDataTypeSerializer + new HiveMetadataSerializer + new HiveStructTypeSerializer

    lazy val delta_util = new DeltaUtil()
    lazy val tree_util = new TreeUtil(tree_address)
    lazy val iceberg_util = new IcebergUtil()

    var delta_txn : Option[OptimisticTransaction] = None
    var iceberg_txn : Option[Transaction] = None
    var tree_txn : Option[TreeTxn] = None

    // Have the options for the tables as well.
    var delta_table : Option[DeltaTableV2] = None
    var iceberg_table : Option[SparkTable] = None
    var tree_table : Option[CatalogTable] = None

    // template data file for Iceberg
    var iceberg_template_data_file : Option[DataFile] = None

    def run() : Unit = {
      var stop = false

      val pb = new ProcessBuilder("python3", misc_config("execScript"), config, catalog_type_str)
      val exec_process = pb.start()
      val reader = new BufferedReader(new InputStreamReader(exec_process.getInputStream))
      val writer = new BufferedWriter(new OutputStreamWriter(exec_process.getOutputStream))

      while (!stop) {
        val meta_op_str = reader.readLine()
        val meta_op_json = json_parser.readTree(meta_op_str)
        val response = meta_op_json.get(0).asText() match {
          case "getTable" => Some(getTable(meta_op_json.get(1).asText(), meta_op_json.get(2)
            .asText()) + "\n")
          case "append" => Some(append(meta_op_json.get(1)) + "\n")
          case _ => None
        }
        if (response.isDefined) {
          writer.write(response.get)
          writer.flush()
        }
        else {
          stop = true
        }
      }

      writer.close()
      reader.close()
      exec_process.waitFor()
      exec_process.destroy()
    }

    private def getTable(db_name : String, table_name : String) : String = {
      catalog_type match {
        case 0 => getDeltaTable(db_name, table_name)
        case 1 => getIcebergTable(db_name, table_name)
        case 2 => getTreeTable(db_name, table_name)
        case _ => ""
      }
    }

    private def append(file_json : JsonNode) : String = {
      catalog_type match {
        case 0 => appendDelta(file_json)
        case 1 => appendIceberg(file_json)
        case 2 => appendTree(file_json)
        case _ => ""
      }
    }

    private def getDeltaTable(db_name : String, table_name : String) : String = {
      delta_table = Some(delta_util.delta.loadTable(Identifier.of(Array(db_name), table_name))
        .asInstanceOf[DeltaTableV2])
      delta_txn = Some(delta_table.get.deltaLog.startTransaction())

      "{ \"location\" : " + "\"" + delta_table.get.catalogTable.get.location.toString + "\", " +
        "\"schema\" :" + Serialization.write(delta_table.get.schema()) + " }"
    }

    private def appendDelta(file_json : JsonNode) : String = {
      // extract partition values from the file json
      val partition_values = new mutable.HashMap[String, String]()

      val add_files = ArrayBuffer(AddFile(file_json.get("path").asText(), partition_values.toMap,
        file_json.get("size").asLong(), file_json.get("modificationTime").asLong(),
        dataChange = false))
      try {
        delta_txn.get.commit(add_files, DeltaOperations.Write(SaveMode.Append))
        "[ true ]"
      } catch {
        case e: Throwable =>
          "[ false ]"
      }
   }

    private def getIcebergTable(db_name : String, table_name : String) : String = {
      iceberg_table = Some(iceberg_util.iceberg
        .loadTable(Identifier.of(Array(db_name), table_name)).asInstanceOf[SparkTable])

      // initiialize the template data file if it does not exist
      if (!iceberg_template_data_file.isDefined) {
        val iceberg_plan_files = iceberg_table.get.table().newScan().planFiles()
        iceberg_plan_files.forEach{ plan_file =>
          iceberg_template_data_file = Some(plan_file.file())
        }
      }

      iceberg_txn = Some(iceberg_table.get.table().newTransaction())
      "{ \"location\" : " + "\"" + iceberg_table.get.table().location() + "\", " +
        "\"schema\" :" + Serialization.write(iceberg_table.get.schema()) + " }"
    }

    private def appendIceberg(file_json : JsonNode) : String = {
      val append_file = new NewGenericDataFile(
        iceberg_template_data_file.get.specId(),
        file_json.get("path").asText(),
        iceberg_template_data_file.get.format(),
        null,
        file_json.get("size").asLong(),
        iceberg_template_data_file.get.recordCount(),
        iceberg_template_data_file.get.columnSizes(),
        iceberg_template_data_file.get.valueCounts(),
        iceberg_template_data_file.get.nullValueCounts(),
        iceberg_template_data_file.get.nanValueCounts(),
        iceberg_template_data_file.get.lowerBounds(),
        iceberg_template_data_file.get.upperBounds(),
        iceberg_template_data_file.get.splitOffsets(),
        iceberg_template_data_file.get.sortOrderId(),
        iceberg_template_data_file.get.keyMetadata()
      )

      // commit all the changes to the table
      iceberg_txn.get.newFastAppend().appendFile(append_file).commit()
      try {
        iceberg_txn.get.commitTransaction()
        "[ true ]"
      } catch {
        case e: Throwable =>
          "[ false ]"
      }

    }

    private def getTreeTable(db_name : String, table_name : String) : String = {
      tree_txn = tree_util.tree.startTransaction(Grpccatalog.TxnMode.TXN_MODE_READ_WRITE)
      tree_table = Some(tree_util.tree.getTable(db_name, table_name, tree_txn))

      "{ \"location\" : " + "\"" + tree_table.get.location.toString + "\", " +
        "\"schema\" :" + Serialization.write(tree_table.get.schema) + " }"
    }

    private def appendTree(file_json : JsonNode) : String = {
      val partition_values = new mutable.HashMap[String, String]()
      val file_path = file_json.get("path").asText()
      val storage = CatalogStorageFormat(Some(new URI(file_path)),
        tree_table.get.storage.inputFormat, tree_table.get.storage.outputFormat,
        tree_table.get.storage.serde, false, tree_table.get.properties)

      val add_files = ArrayBuffer(CatalogTableFile(storage, partition_values.toMap,
        file_json.get("size").asLong, file_json.get("modificationTime").asLong))

      tree_util.tree.addFiles(tree_table.get, add_files, tree_txn)
      val success = tree_util.tree.commit(tree_txn.get)
      if (success) {
        "[ true ]"
      }
      else {
        "[ false ]"
      }
    }
  }

}

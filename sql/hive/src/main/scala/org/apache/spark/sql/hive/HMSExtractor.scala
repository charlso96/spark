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

package org.apache.spark.sql.hive

import java.io.{File, FileWriter}
import java.net.URI

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.util.matching.Regex

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JNull
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableFile, CatalogTablePartition, CatalogTypes}
import org.apache.spark.sql.types.{DataType, IntegralType, Metadata, StructField, StructType}
import org.apache.spark.util._


private[spark] class HMSClientExt(args: Seq[String], env:
  scala.collection.immutable.Map[String, String] = sys.env)
  extends Logging {
  private implicit val formats = Serialization.formats(NoTypeHints) + new HiveURISerializer +
    new HiveDataTypeSerializer + new HiveMetadataSerializer + new HiveStructTypeSerializer
  var propertiesFile: String = null
  var verbose: Boolean = false
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    if (verbose) {
      logInfo(s"Using properties file: $propertiesFile")
    }
    Option(propertiesFile).foreach { filename =>
      val properties = Utils.getPropertiesFromFile(filename)
      properties.foreach { case (k, v) =>
        defaultProperties(k) = v
      }
      // Property files may contain sensitive information, so redact before printing
      if (verbose) {
        Utils.redact(properties).foreach { case (k, v) =>
          logInfo(s"Adding default property: $k=$v")
        }
      }
    }
    defaultProperties
  }
  mergeDefaultSparkProperties()

  ignoreNonSparkProperties()

  private val sparkConf = toSparkConf()

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)

  lazy val client = new HiveExternalCatalog(sparkConf, hadoopConf)

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

  // have to cast as case class of Datatype for each one
//  private class HiveArrayTypeSerializer extends CustomSerializer[ArrayType](format =>
//    (
//      {
//        case JObject(o) => DataType.parseDataType(JObject(o)).asInstanceOf[ArrayType]
//        case JNull => null
//      },
//      { case x: ArrayType =>
//        x.jsonValue
//      }
//    )
//  )
//
//  private class HiveMapTypeSerializer extends CustomSerializer[MapType](format =>
//    (
//      {
//        case JObject(o) => DataType.parseDataType(JObject(o)).asInstanceOf[MapType]
//        case JNull => null
//      },
//      { case x: MapType =>
//        x.jsonValue
//      }
//    )
//  )

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

//  private class HiveUserDefinedTypeSerializer extends
//    CustomSerializer[UserDefinedType[Any]](format =>
//    (
//      {
//        case JObject(o) => DataType.parseDataType(JObject(o)).asInstanceOf[UserDefinedType[Any]]
//        case JNull => null
//      },
//      { case x: UserDefinedType[Any] =>
//        x.jsonValue
//      }
//    )
//  )
//
//  private class HivePythonUserDefinedTypeSerializer extends
//    CustomSerializer[PythonUserDefinedType](format =>
//    (
//      {
//        case JObject(o) => DataType.parseDataType(JObject(o)).asInstanceOf[PythonUserDefinedType]
//        case JNull => null
//      },
//      { case x: PythonUserDefinedType =>
//        x.jsonValue
//      }
//    )
//  )

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

  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.keys.foreach { k =>
      if (!k.startsWith("spark.")) {
        sparkProperties -= k
        logWarning(s"Ignoring non-Spark config property: $k")
      }
    }
  }

  private def toSparkConf(sparkConf: Option[SparkConf] = None): SparkConf = {
    // either use an existing config or create a new empty one
    sparkProperties.foldLeft(sparkConf.getOrElse(new SparkConf())) {
      case (conf, (k, v)) => conf.set(k, v)
    }
  }

  def getTable(db_name : String, table_name : String) : CatalogTable = {
    client.getTable(db_name, table_name)
  }

  def getDBJson(db_name : String) : String = {
    val database = client.getDatabase(db_name)
    val db_prefix = "{\"meta\": {\"paths\": [\"/" + db_name +
      "\"], \"obj_kind\": 0}, \"val\": {\"obj_type\": \"database\", "
    val json_brace : Regex = "\\{".r
    val db_json = Serialization.write(database)
    json_brace.replaceFirstIn(db_json, db_prefix) + "}"
  }

  def getTableJson(table : CatalogTable, obj_kind : Int) : String = {
    val table_prefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get +
      "/" + table.identifier.table + "\"], \"obj_kind\": " + obj_kind.toString +
      "}, \"val\": {\"obj_type\": \"table\", "
    val json_brace : Regex = "\\{".r
    val table_json = Serialization.write(table)
    json_brace.replaceFirstIn(table_json, table_prefix) + "}"
  }

  def getPartitionJson(table : CatalogTable, partition :
    CatalogTablePartition, obj_kind : Int) : String = {
    val partition_prefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get +
      "/" + table.identifier.table + getPartId(table, partition) + "\"], \"obj_kind\": " +
      obj_kind.toString + "}," + "\"val\": {\"obj_type\": \"partition\", "
    val json_brace : Regex = "\\{".r
    val partition_json = Serialization.write(partition)
    json_brace.replaceFirstIn(partition_json, partition_prefix) + "}"
  }

  def getFileJson(table : CatalogTable, file : FileStatus) : String = {
    val file_prefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get + "/" +
      table.identifier.table + getFileId(file) + "\"], \"obj_kind\": 2}, " +
      "\"val\": {\"obj_type\": \"file\", "
    val fileStorage = CatalogStorageFormat(Some(new URI(file.getPath.toString)),
      table.storage.inputFormat, table.storage.outputFormat, table.storage.serde,
      table.storage.compressed, table.storage.properties)
    val fileObj = CatalogTableFile(fileStorage, Map.empty[String, String].toMap,
      file.getLen, file.getModificationTime, None, Map.empty[String, String].toMap)
    val json_brace : Regex = "\\{".r
    val file_json = Serialization.write(fileObj)
    json_brace.replaceFirstIn(file_json, file_prefix) + "}"
  }
  def getFileJson(table : CatalogTable, partition : CatalogTablePartition,
                  file : FileStatus) : String = {
    val file_prefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get + "/" +
      table.identifier.table + getPartId(table, partition) + getFileId(file) +
      "\"], \"obj_kind\": 2}, " +
      "\"val\": {\"obj_type\": \"file\", "
    val fileStorage = CatalogStorageFormat(Some(new URI(file.getPath.toString)),
      partition.storage.inputFormat, partition.storage.outputFormat, partition.storage.serde,
      partition.storage.compressed, partition.storage.properties)
    val fileObj = CatalogTableFile(fileStorage, partition.spec,
      file.getLen, file.getModificationTime, None, Map.empty[String, String].toMap)
    val json_brace : Regex = "\\{".r
    val file_json = Serialization.write(fileObj)
    json_brace.replaceFirstIn(file_json, file_prefix) + "}"
  }

  def listTables(db_name : String) : Seq[String] = {
    client.listTables(db_name)
  }

  def listPartitions(db_name : String, table_name : String) : Seq[CatalogTablePartition] = {
    client.listPartitions(db_name, table_name, None)
  }

  def listFiles(table : CatalogTable) : Seq[FileStatus] = {
    val table_path = new Path(table.location)
    val fs = table_path.getFileSystem(hadoopConf)
    fs.listStatus(table_path).toSeq
  }

  def listFiles(partition : CatalogTablePartition): Seq[FileStatus] = {
    val partition_path = new Path(partition.location)
    val fs = partition_path.getFileSystem(hadoopConf)
    fs.listStatus(partition_path).toSeq
  }

  private def getPartId(table : CatalogTable, partition : CatalogTablePartition): String = {
    var partId = ""

    table.partitionSchema.foreach { partitionColumn =>
      if (partition.spec.contains(partitionColumn.name)) {
        val partitionVal = getPartitionVal(partitionColumn, partition.spec)
        partId = partId + "/" + partitionColumn.name + "=" + partitionVal
      }
    }

    partId
  }

  private def getFileId(file : FileStatus) : String = {
    val file_path = file.getPath.toString
    file_path.slice(file_path.lastIndexOf('/'), file_path.size)
  }

  // precondition: corresponding partition val exists
  private def getPartitionVal(partitionColumn: StructField,
                              partitionSpec : CatalogTypes.TablePartitionSpec): String = {
    val partitionVal = partitionSpec.get(partitionColumn.name).get
    if (partitionColumn.dataType.isInstanceOf[IntegralType]) {
      "%012d".format(partitionVal.toLong)
    }
    else {
      partitionVal
    }
  }

}
/**
 * Main function of the extractor
 */
private[spark] object HMSExtractor extends Logging {

  def main(args: Array[String]): Unit = {

    val hms_ext = new HMSClientExt(args.toSeq)
    /**
     * OK, just get a single table for now...
     * OK, don't worry about statistics for now...
     */

    val db_name = args(0)
    val file_writer = new FileWriter(new File(args(1)))
    // write the db object
    val db_json = hms_ext.getDBJson(db_name)
    file_writer.write(db_json + "\n")

    val table_names = hms_ext.listTables(db_name)
    table_names.foreach { table_name =>
      val table = hms_ext.getTable(db_name, table_name)
      // non-partitioned table
      if (table.partitionColumnNames.isEmpty) {
        val table_json = hms_ext.getTableJson(table, 1)
        file_writer.write(table_json + "\n")
        val files = hms_ext.listFiles(table)
        files.foreach { file =>
          val file_json = hms_ext.getFileJson(table, file)
          file_writer.write(file_json + "\n")
        }
      }
      // partitioned table
      else {
        val table_json = hms_ext.getTableJson(table, 0)
        file_writer.write(table_json + "\n")
        val partitions = hms_ext.listPartitions(db_name, table_name)
          .sortWith( compPartition(table.partitionColumnNames, _, _) )

        val cur_part_vals = new ArrayBuffer[String]
        table.partitionColumnNames.foreach { part_col =>
          cur_part_vals += ""
        }

        val emptyStorage = CatalogStorageFormat(None, None, None, None,
            false, Map.empty[String, String].toMap)
        partitions.foreach { partition =>
          var diff = false
          // partition spec to build
          val new_part_spec = Map.empty[String, String]
          table.partitionColumnNames.indices.foreach { i =>
            new_part_spec.put(table.partitionColumnNames(i), partition.
              spec(table.partitionColumnNames(i)))
            if (!diff &&
              partition.spec(table.partitionColumnNames(i)) != cur_part_vals(i)) {
              diff = true
              table.partitionColumnNames.indices.slice(i,
                table.partitionColumnNames.size - 1).foreach { j =>
                new_part_spec.put(table.partitionColumnNames(j), partition.
                  spec(table.partitionColumnNames(j)))
                cur_part_vals(j) = partition.spec(table.partitionColumnNames(j))
                val new_part = CatalogTablePartition(new_part_spec.toMap,
                  emptyStorage, Map.empty[String, String].toMap, partition.createTime, -1, None)
                val partition_json = hms_ext.getPartitionJson(table, new_part, 0)
                file_writer.write(partition_json + "\n")
              }
            }
          }
          cur_part_vals(table.partitionColumnNames.size - 1) = partition.spec(table.
            partitionColumnNames(table.partitionColumnNames.size - 1))
          val partition_json = hms_ext.getPartitionJson(table, partition, 1)
          file_writer.write(partition_json + "\n")
          val files = hms_ext.listFiles(partition)
          files.foreach { file =>
            val file_json = hms_ext.getFileJson(table, partition, file)
            file_writer.write(file_json + "\n")
          }
        }
      }
    }
    file_writer.close()
  }

  private def compPartition(part_cols : Seq[String], part1 : CatalogTablePartition,
                            part2 : CatalogTablePartition): Boolean = {
    var comp_val = 0
    part_cols.foreach { part_col =>
      if (comp_val == 0 && part1.spec(part_col) < part2.spec(part_col)) {
        comp_val = -1
      }
      else if (comp_val == 0 && part1.spec(part_col) > part2.spec(part_col)) {
        comp_val = 1
      }
    }

    if (comp_val == -1 || comp_val == 0) {
      true
    }
    else {
      false
    }
  }
}

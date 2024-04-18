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

import scala.collection.mutable.HashMap
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
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.util._


private[hive] class HMSClientExt(args: Seq[String], env: Map[String, String] = sys.env)
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
    val db_prefix = "{\"obj_type\" : \"database\","
    val json_brace : Regex = "\\{".r
    val db_json = Serialization.write(database)
    json_brace.replaceFirstIn(db_json, db_prefix)

  }

  def getTableJson(db_name : String, table_name : String) : String = {
    val table = client.getTable(db_name, table_name)
    val table_prefix = "{\"obj_type\" : \"table\","
    val json_brace : Regex = "\\{".r
    val table_json = Serialization.write(table)
    json_brace.replaceFirstIn(table_json, table_prefix)
  }

  def getPartitionJson(partition : CatalogTablePartition) : String = {
    val partition_prefix = "{\"obj_type\" : \"partition\","
    val json_brace : Regex = "\\{".r
    val partition_json = Serialization.write(partition)
    json_brace.replaceFirstIn(partition_json, partition_prefix)
  }

  def getFileJson(file : FileStatus) : String = {
    "{\"obj_type\" : \"file\", \"path\" : \"" + file.getPath.toString +
      "\", \"size\" : " + file.getLen.toString + ", \"modificationTime\" : " +
      file.getModificationTime.toString + "}"
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

}
/**
 * This entry point is used by the launcher library to start in-process Spark applications.
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
    val db_json = hms_ext.getDBJson(db_name)
    file_writer.write(db_json + "\n")

    val table_names = hms_ext.listTables(db_name)
    table_names.foreach { table_name =>
      val table_json = hms_ext.getTableJson(db_name, table_name)
      file_writer.write(table_json + "\n")

      val table = hms_ext.getTable(db_name, table_name)
      if (table.partitionColumnNames.isEmpty) {
        val files = hms_ext.listFiles(table)
        files.foreach { file =>
          val file_json = hms_ext.getFileJson(file)
          file_writer.write(file_json + "\n")
        }
      }
      else {
        val partitions = hms_ext.listPartitions(db_name, table_name)
        partitions.foreach { partition =>
          val partition_json = hms_ext.getPartitionJson(partition)
          file_writer.write(partition_json + "\n")
          val files = hms_ext.listFiles(partition)
          files.foreach { file =>
            val file_json = hms_ext.getFileJson(file)
            file_writer.write(file_json + "\n")
          }
        }
      }
    }
    file_writer.close()
  }
}

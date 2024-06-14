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
import org.bson.BsonBinaryWriter
import org.bson.RawBsonDocument
import org.bson.io.BasicOutputBuffer
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JNull
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTableFile, CatalogTablePartition, CatalogTypes}
import org.apache.spark.sql.types._
import org.apache.spark.util._


private[spark] class HMSClientExt(args: Seq[String], env:
  scala.collection.immutable.Map[String, String] = sys.env)
  extends Logging {
  private val jsonWriterSetting : JsonWriterSettings = JsonWriterSettings.builder().
    outputMode(JsonMode.RELAXED).build()

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
      "/" + table.identifier.table + getPartId(table, partition.spec) + "\"], \"obj_kind\": " +
      obj_kind.toString + "}," + "\"val\": {\"obj_type\": \"partition\", "
    val json_brace : Regex = "\\{".r
    val partition_json = Serialization.write(partition)
    json_brace.replaceFirstIn(partition_json, partition_prefix) + "}"
  }

  def getFileJson(table : CatalogTable, file : FileStatus, stats: Option[CatalogStatistics])
      : String = {
    val file_prefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get + "/" +
      table.identifier.table + getFileId(file) + "\"], \"obj_kind\": 2}, " +
      "\"val\": {"
    val fileStorage = CatalogStorageFormat(Some(new URI(file.getPath.toString)),
      table.storage.inputFormat, table.storage.outputFormat, table.storage.serde,
      table.storage.compressed, table.storage.properties)
    val fileObj = CatalogTableFile(fileStorage, Map.empty[String, String].toMap,
      file.getLen, file.getModificationTime, stats, Map.empty[String, String].toMap)
    val json_brace : Regex = "\\{".r

    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)
    writer.writeStartDocument()
    writer.writeString("obj_type", "file")
    writer.writeName("storage")
    appendBson(writer, fileObj.storage)
    writer.writeName("partitionValues")
    appendBson(writer, fileObj.partitionValues)
    writer.writeInt64("size", fileObj.size)
    writer.writeInt64("modificationTime", fileObj.modificationTime)
    if (fileObj.stats.isDefined) {
      writer.writeName("stats")
      appendBson(writer, table, fileObj.stats.get)
    }
    writer.writeName("tags")
    appendBson(writer, fileObj.tags)
    writer.writeEndDocument()

    val fileJson = new RawBsonDocument(outputBuffer.getInternalBuffer, 0, outputBuffer.getPosition)
      .toJson(jsonWriterSetting)
    json_brace.replaceFirstIn(fileJson, file_prefix) + "}"
  }

  def getFileJson(table : CatalogTable, partition : CatalogTablePartition,
                  file : FileStatus, stats: Option[CatalogStatistics]) : String = {
    val file_prefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get + "/" +
      table.identifier.table + getPartId(table, partition.spec) + getFileId(file) +
      "\"], \"obj_kind\": 2}, " +
      "\"val\": {"
    val fileStorage = CatalogStorageFormat(Some(new URI(file.getPath.toString)),
      partition.storage.inputFormat, partition.storage.outputFormat, partition.storage.serde,
      partition.storage.compressed, partition.storage.properties)
    val fileObj = CatalogTableFile(fileStorage, partition.spec,
      file.getLen, file.getModificationTime, stats, Map.empty[String, String].toMap)
    val json_brace : Regex = "\\{".r

    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)
    writer.writeStartDocument()
    writer.writeString("obj_type", "file")
    writer.writeName("storage")
    appendBson(writer, fileObj.storage)
    writer.writeName("partitionValues")
    appendBson(writer, fileObj.partitionValues)
    writer.writeInt64("size", fileObj.size)
    writer.writeInt64("modificationTime", fileObj.modificationTime)
    if (fileObj.stats.isDefined) {
      writer.writeName("stats")
      appendBson(writer, table, fileObj.stats.get)
    }
    writer.writeName("tags")
    appendBson(writer, fileObj.tags)
    writer.writeEndDocument()

    val fileJson = new RawBsonDocument(outputBuffer.getInternalBuffer, 0, outputBuffer.getPosition)
      .toJson(jsonWriterSetting)
    json_brace.replaceFirstIn(fileJson, file_prefix) + "}"

  }

  private def appendBson(writer : BsonBinaryWriter, storage : CatalogStorageFormat) : Unit = {
    writer.writeStartDocument()
    if (storage.locationUri.isDefined) {
      writer.writeString("locationUri", storage.locationUri.get.toString)
    }
    if (storage.inputFormat.isDefined) {
      writer.writeString("inputFormat", storage.inputFormat.get)
    }
    if (storage.outputFormat.isDefined) {
      writer.writeString("outputFormat", storage.outputFormat.get)
    }
    if (storage.serde.isDefined) {
      writer.writeString("serde", storage.serde.get)
    }
    writer.writeBoolean("compressed", storage.compressed)
    writer.writeName("properties")
    appendBson(writer, storage.properties)
    writer.writeEndDocument()
  }

  private def appendBson(writer : BsonBinaryWriter, map :
      scala.collection.immutable.Map[String, String]) : Unit = {
    writer.writeStartDocument()
    map.foreach{ case (key, value) =>
      writer.writeString(key, value)
    }
    writer.writeEndDocument()
  }

  private def appendBson(writer : BsonBinaryWriter, table: CatalogTable,
      stats : CatalogStatistics) : Unit = {
    writer.writeStartDocument()
    writer.writeInt64("sizeInBytes", stats.sizeInBytes.toLong)
    if (stats.rowCount.isDefined) {
      writer.writeInt64("rowCount", stats.rowCount.get.toLong)
    }
    writer.writeName("colStats")
    writer.writeStartDocument()

    table.schema.fields.foreach{ field =>
      val colStat = stats.colStats.get(field.name)
      if (colStat.isDefined) {
        writer.writeName(field.name)
        appendBson(writer, field.dataType, colStat.get)
      }
    }

    writer.writeEndDocument()
    writer.writeEndDocument()
  }

  def getStatsJson(table: CatalogTable, partitionSpec: CatalogTypes.TablePartitionSpec,
                   stats: CatalogStatistics) : String = {
    val statsPrefix = "{\"meta\": {\"paths\": [\"/" + table.identifier.database.get + "/" +
      table.identifier.table + getPartId(table, partitionSpec) + "/stats" +
      "\"], \"obj_kind\": 0}, \"val\": {\"obj_type\": \"stats\", "
    val jsonBrace : Regex = "\\{".r

    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)
      writer.writeStartDocument()
      writer.writeInt64("sizeInBytes", stats.sizeInBytes.toLong)
      if (stats.rowCount.isDefined) {
        writer.writeInt64("rowCount", stats.rowCount.get.toLong)
      }
      writer.writeName("colStats")
      writer.writeStartDocument()
      table.schema.fields.foreach{ field =>
        val colStat = stats.colStats.get(field.name)
        if (colStat.isDefined) {
          writer.writeName(field.name)
          appendBson(writer, field.dataType, colStat.get)
        }
      }
      writer.writeEndDocument()

    writer.writeEndDocument()

    val statsJson = new RawBsonDocument(outputBuffer.getInternalBuffer, 0, outputBuffer.getPosition)
      .toJson(jsonWriterSetting)
    jsonBrace.replaceFirstIn(statsJson, statsPrefix) + "}"
  }

  private def appendBson(writer : BsonBinaryWriter, dataType: DataType, colStat: CatalogColumnStat):
  Unit = {
    writer.writeStartDocument()
    if (colStat.distinctCount.isDefined) {
      writer.writeInt64("distinctCount", colStat.distinctCount.get.toLong)
    }
    if (colStat.min.isDefined) {
      dataType match {
        case ByteType => writer.writeInt32("min", colStat.min.get.toInt)
        case ShortType => writer.writeInt32("min", colStat.min.get.toInt)
        case IntegerType => writer.writeInt32("min", colStat.min.get.toInt)
        case LongType => writer.writeInt64("min", colStat.min.get.toLong)
        case BooleanType => writer.writeBoolean("min", colStat.min.get.toBoolean)
        case FloatType => writer.writeDouble("min", colStat.min.get.toDouble)
        case DoubleType => writer.writeDouble("min", colStat.min.get.toDouble)
        case _ => writer.writeString("min", colStat.min.get)
      }
    }
    if (colStat.max.isDefined) {
      dataType match {
        case ByteType => writer.writeInt32("max", colStat.max.get.toInt)
        case ShortType => writer.writeInt32("max", colStat.max.get.toInt)
        case IntegerType => writer.writeInt32("max", colStat.max.get.toInt)
        case LongType => writer.writeInt64("max", colStat.max.get.toLong)
        case BooleanType => writer.writeBoolean("max", colStat.max.get.toBoolean)
        case FloatType => writer.writeDouble("max", colStat.max.get.toDouble)
        case DoubleType => writer.writeDouble("max", colStat.max.get.toDouble)
        case _ => writer.writeString("max", colStat.max.get)
      }
    }
    if (colStat.nullCount.isDefined) {
      writer.writeInt64("nullCount", colStat.nullCount.get.toLong)
    }
    if (colStat.avgLen.isDefined) {
      writer.writeInt64("avgLen", colStat.avgLen.get)
    }
    if (colStat.maxLen.isDefined) {
      writer.writeInt64("maxLen", colStat.maxLen.get)
    }
    // no support for histogram
    writer.writeInt32("version", colStat.version)

    writer.writeEndDocument()
  }

  // helper function for merging partition values into table statistics
  def mergePartitionVals(table: CatalogTable, base : CatalogStatistics, partitionSpec :
                        CatalogTypes.TablePartitionSpec): CatalogStatistics = {
    val colStats = Map(base.colStats.toSeq: _ *)
    table.partitionSchema.foreach{ partCol =>
      val delta = CatalogColumnStat(None, Some(partitionSpec(partCol.name)),
        Some(partitionSpec(partCol.name)), None, None, None, None, 1)

      val mergedColStat = mergeColStats(partCol, base.colStats.get(partCol.name),
        Some(delta))
      if (mergedColStat.isDefined) {
        colStats.put(partCol.name, mergedColStat.get)
      }
    }
    CatalogStatistics(base.sizeInBytes, base.rowCount, colStats.toMap)

  }

  def mergeStats(table : CatalogTable, base : CatalogStatistics,
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
    val colStats = Map.empty[String, CatalogColumnStat]
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
            if (attr.dataType.isInstanceOf[IntegralType]) {
              Some(base.get.min.get.toInt.min(delta.get.min.get.toInt).toString)
            }
            // simple string comparison takes care of both string and dates
            else {
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
            if (attr.dataType.isInstanceOf[IntegralType]) {
              Some(base.get.max.get.toInt.max(delta.get.max.get.toInt).toString)
            }
            // simple string comparison takes care of both string and dates
            else {
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
  // TODO: we only assume 1 row per file for now
  def extractStats(table : CatalogTable, file : FileStatus): CatalogStatistics = {
    val fs = file.getPath.getFileSystem(hadoopConf)
    val dataBuf: Array[Byte] = new Array[Byte](file.getLen.toInt)
    val ifstream = fs.open(file.getPath)
    ifstream.readFully(dataBuf)
    ifstream.close()
    val schema = table.schema
    val record = dataBuf.map(_.toChar).mkString.replaceAll("[\n\r]", "").split(",")
    val colStats = Map.empty[String, CatalogColumnStat]
    record.indices.foreach{ i =>
      colStats.put(schema(i).name, CatalogColumnStat(None, Some(record(i)), Some(record(i)),
        Some(0), None, None, None, 1))
    }
    CatalogStatistics(file.getLen, Some(1), colStats.toMap)
  }

  private def getPartId(table : CatalogTable, partitionSpec : CatalogTypes.TablePartitionSpec)
      : String = {
    var partId = ""
    table.partitionSchema.foreach { partitionColumn =>
      if (partitionSpec.contains(partitionColumn.name)) {
        val partitionVal = getPartitionVal(partitionColumn, partitionSpec)
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
    val db_name = args(0)
    val file_writer = new FileWriter(new File(args(1)))
    // write the db object
    val db_json = hms_ext.getDBJson(db_name)
    file_writer.write(db_json + "\n")

    val table_names = hms_ext.listTables(db_name)
    table_names.foreach { table_name =>
      val table = hms_ext.getTable(db_name, table_name)
      var tableStats = CatalogStatistics(BigInt(0), Some(BigInt(0)),
          Map.empty[String, CatalogColumnStat].toMap)
      // non-partitioned table
      if (table.partitionColumnNames.isEmpty) {
        val table_json = hms_ext.getTableJson(table, 1)
        file_writer.write(table_json + "\n")
        val files = hms_ext.listFiles(table)
        files.foreach { file =>
          val file_stats = hms_ext.extractStats(table, file)
          tableStats = hms_ext.mergeStats(table, tableStats, file_stats)
          val file_json = hms_ext.getFileJson(table, file, Some(file_stats))
          file_writer.write(file_json + "\n")
        }
      }
      // partitioned table
      else {
        val table_json = hms_ext.getTableJson(table, 0)
        file_writer.write(table_json + "\n")
        val partitions = hms_ext.listPartitions(db_name, table_name)
          .sortWith( compPartition(table.partitionColumnNames, _, _) )
        // array for keeping track of current partition value at each level
        val cur_part_vals = new ArrayBuffer[String]
        table.partitionColumnNames.foreach { part_col =>
          cur_part_vals += ""
        }
        // array for keeping track of current statistics at each level
        val cur_part_stats = new ArrayBuffer[CatalogStatistics]

        val emptyStorage = CatalogStorageFormat(None, None, None, None,
            false, Map.empty[String, String].toMap)
        partitions.foreach { partition =>
          var diff = false
          // new partition spec to build
          val new_part_spec = Map.empty[String, String]
          // previous partition spec for which statistics have to be flushed before new partition
          // is added
          val prev_part_spec = Map.empty[String, String]
          table.partitionColumnNames.indices.foreach { i =>
            new_part_spec.put(table.partitionColumnNames(i), partition.
              spec(table.partitionColumnNames(i)))
            prev_part_spec.put(table.partitionColumnNames(i), cur_part_vals(i))
            if (!diff && partition.spec(table.partitionColumnNames(i)) != cur_part_vals(i)) {
              diff = true
              table.partitionColumnNames.indices.slice(i,
                table.partitionColumnNames.size - 1).foreach { j =>
                new_part_spec.put(table.partitionColumnNames(j), partition.
                  spec(table.partitionColumnNames(j)))
                prev_part_spec.put(table.partitionColumnNames(j), cur_part_vals(j))
                cur_part_vals(j) = partition.spec(table.partitionColumnNames(j))
                val new_part = CatalogTablePartition(new_part_spec.toMap,
                  emptyStorage, Map.empty[String, String].toMap, partition.createTime, -1, None)
                val partition_json = hms_ext.getPartitionJson(table, new_part, 0)
                // before new partition is added, stats of the previous partition is flushed out
                if (cur_part_stats.size > j) {
                  // write the previous stat and replace it with empty stat
                  val stats_json = hms_ext.getStatsJson(table, prev_part_spec.toMap,
                      cur_part_stats(j))
                  cur_part_stats(j) = CatalogStatistics(BigInt(0), Some(BigInt(0)),
                      Map.empty[String, CatalogColumnStat].toMap)
                  file_writer.write(stats_json + "\n")
                }
                else {
                  // add empty stat object to the stats array
                  cur_part_stats.append(CatalogStatistics(BigInt(0), Some(BigInt(0)),
                    Map.empty[String, CatalogColumnStat].toMap))
                }

                file_writer.write(partition_json + "\n")
              }
            }
          }
          cur_part_vals(table.partitionColumnNames.size - 1) = partition.spec(table.
            partitionColumnNames(table.partitionColumnNames.size - 1))
          val partition_json = hms_ext.getPartitionJson(table, partition, 1)
          file_writer.write(partition_json + "\n")
          val files = hms_ext.listFiles(partition)
          var leafStats = CatalogStatistics(BigInt(0), Some(BigInt(0)),
              Map.empty[String, CatalogColumnStat].toMap)
          files.foreach { file =>
            val file_stats = hms_ext.extractStats(table, file)
            cur_part_stats.indices.foreach { i =>
              cur_part_stats(i) = hms_ext.mergeStats(table, cur_part_stats(i), file_stats)

            }
            tableStats = hms_ext.mergeStats(table, tableStats, file_stats)
            leafStats = hms_ext.mergeStats(table, leafStats, file_stats)

            val file_json = hms_ext.getFileJson(table, partition, file, Some(file_stats))
            file_writer.write(file_json + "\n")
          }
          // merge partition values into table statistics
          tableStats = hms_ext.mergePartitionVals(table, tableStats, partition.spec)

          val stats_json = hms_ext.getStatsJson(table, partition.spec, leafStats)
          file_writer.write(stats_json + "\n")
        }
        // flush out partition stats that has not been written out to json
        val partVals = Map.empty[String, String]
        cur_part_stats.indices.foreach { i =>
          val partStats = cur_part_stats(i)
          partVals.put(table.partitionColumnNames(i), cur_part_vals(i))
          // if the stats object is not empty, flush it out
          if (!( partStats.sizeInBytes == BigInt(0) && partStats.rowCount.get == BigInt(0) &&
              partStats.colStats.isEmpty)) {
            val stats_json = hms_ext.getStatsJson(table, partVals.toMap, cur_part_stats(i))
            file_writer.write(stats_json + "\n")
          }
        }
      }

      val tableStatsJson = hms_ext.getStatsJson(table, Map.empty[String, String].toMap, tableStats)
      file_writer.write(tableStatsJson + "\n")
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

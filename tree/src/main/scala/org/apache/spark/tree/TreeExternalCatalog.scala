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

package org.apache.spark.tree

import java.net.URI
import java.nio.ByteBuffer

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.bson.BsonArray
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonType
import org.bson.BsonValue
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.types._
import org.apache.spark.tree.grpc.Grpccatalog
import org.apache.spark.tree.grpc.Grpccatalog._
import org.apache.spark.tree.grpc.Grpccatalog.Predicate
import org.apache.spark.tree.grpc.GRPCCatalogGrpc
import org.apache.spark.unsafe.types.UTF8String

// custom serde that should be much faster than jackson
private[spark] object TreeSerde {
  private val jsonWriterSetting : JsonWriterSettings = JsonWriterSettings.builder().
    outputMode(JsonMode.RELAXED).build()

  // serializer for CatalogTablePartition
  def toBson(objPath : String, table : CatalogTable,
             partition : CatalogTablePartition): BasicOutputBuffer = {
    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)
    writer.writeStartDocument()

    writer.writeStartDocument("meta")
    writer.writeStartArray("paths")
    writer.writeString(objPath)
    writer.writeEndArray()
    writer.writeEndDocument()

    writer.writeStartDocument("val")
    writer.writeString("obj_type", "partition")
    writer.writeName("spec")
    appendBson(writer, partition.spec)
    writer.writeName("storage")
    appendBson(writer, partition.storage)
    writer.writeName("parameters")
    appendBson(writer, partition.parameters)
    writer.writeInt64("createTime", partition.createTime)
    writer.writeInt64("lastAccessTime", partition.lastAccessTime)
    if (partition.stats.isDefined) {
      writer.writeName("stats")
      appendBson(writer, table, partition.stats.get)
    }
    writer.writeEndDocument()

    writer.writeEndDocument()
    outputBuffer
  }

  // serializer for CatalogTableFile
  def toBson(objPath: String, table: CatalogTable,
             file : CatalogTableFile) : BasicOutputBuffer = {
    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)
    writer.writeStartDocument()

    writer.writeStartDocument("meta")
    writer.writeStartArray("paths")
    writer.writeString(objPath)
    writer.writeEndArray()
    writer.writeEndDocument()

    writer.writeStartDocument("val")
    writer.writeString("obj_type", "file")
    writer.writeName("storage")
    appendBson(writer, file.storage)
    writer.writeName("partitionValues")
    appendBson(writer, file.partitionValues)
    writer.writeInt64("size", file.size)
    writer.writeInt64("modificationTime", file.modificationTime)
    if (file.stats.isDefined) {
      writer.writeName("stats")
      appendBson(writer, table, file.stats.get)
    }
    writer.writeName("tags")
    appendBson(writer, file.tags)
    writer.writeEndDocument()

    writer.writeEndDocument()
    outputBuffer
  }

  // serializer for CatalogStatistics
  def toBson(objPath: String, table: CatalogTable, stats: CatalogStatistics) : BasicOutputBuffer = {
    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)
    writer.writeStartDocument()

    writer.writeStartDocument("meta")
    writer.writeStartArray("paths")
    writer.writeString(objPath)
    writer.writeEndArray()
    writer.writeEndDocument()

    writer.writeStartDocument("val")
    writer.writeString("obj_type", "stats")
    writer.writeInt64("sizeInBytes", stats.sizeInBytes.toLong)
    if (stats.rowCount.isDefined) {
      writer.writeInt64("rowCount", stats.rowCount.get.toLong)
    }

    writer.writeStartDocument("colStats")
    table.schema.fields.foreach{ field =>
      val colStat = stats.colStats.get(field.name)
      if (colStat.isDefined) {
        writer.writeName(field.name)
        appendBson(writer, field.dataType, colStat.get)
      }
    }
    writer.writeEndDocument()
    writer.writeEndDocument()

    writer.writeEndDocument()
    outputBuffer
  }

  // 4 increment
  // 5 decrement
  // 6 min
  // 7 max
  def toMergeBson(table: CatalogTable, stats: CatalogStatistics) : BasicOutputBuffer = {
    val outputBuffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(outputBuffer)

    writer.writeStartDocument()

    writer.writeStartDocument("sizeInBytes")
    writer.writeString("op", "4")
    writer.writeInt64("", stats.sizeInBytes.toLong)
    writer.writeEndDocument()

    if (stats.rowCount.isDefined) {
      writer.writeStartDocument("rowCount")
      writer.writeString("op", "4")
      writer.writeInt64("", stats.rowCount.get.toLong)
      writer.writeEndDocument()
    }

    writer.writeStartDocument("colStats")
    table.schema.fields.foreach{ field =>
      val colStat = stats.colStats.get(field.name)
      if (colStat.isDefined) {
        writer.writeName(field.name)
        appendMergeBson(writer, field.dataType, colStat.get)
      }
    }
    writer.writeEndDocument()

    writer.writeEndDocument()
    outputBuffer
  }

  private def appendMergeBson(writer : BsonBinaryWriter, dataType: DataType,
                              colStat : CatalogColumnStat): Unit = {
    writer.writeStartDocument()
    // no merge operation for distinct count
    if (colStat.min.isDefined) {
      writer.writeStartDocument("min")
      writer.writeString("op", "6")
      dataType match {
        case ByteType => writer.writeInt32("", colStat.min.get.toInt)
        case ShortType => writer.writeInt32("", colStat.min.get.toInt)
        case IntegerType => writer.writeInt32("", colStat.min.get.toInt)
        case LongType => writer.writeInt64("", colStat.min.get.toLong)
        case BooleanType => writer.writeBoolean("", colStat.min.get.toBoolean)
        case FloatType => writer.writeDouble("", colStat.min.get.toDouble)
        case DoubleType => writer.writeDouble("", colStat.min.get.toDouble)
        case _ => writer.writeString("", colStat.min.get)
      }
      writer.writeEndDocument()
    }
    if (colStat.max.isDefined) {
      writer.writeStartDocument("max")
      writer.writeString("op", "7")
      dataType match {
        case ByteType => writer.writeInt32("", colStat.max.get.toInt)
        case ShortType => writer.writeInt32("", colStat.max.get.toInt)
        case IntegerType => writer.writeInt32("", colStat.max.get.toInt)
        case LongType => writer.writeInt64("", colStat.max.get.toLong)
        case BooleanType => writer.writeBoolean("", colStat.max.get.toBoolean)
        case FloatType => writer.writeDouble("", colStat.max.get.toDouble)
        case DoubleType => writer.writeDouble("", colStat.max.get.toDouble)
        case _ => writer.writeString("", colStat.max.get)
      }
      writer.writeEndDocument()
    }
    if (colStat.nullCount.isDefined) {
      writer.writeStartDocument("nullCount")
      writer.writeString("op", "4")
      writer.writeInt64("", colStat.nullCount.get.toLong)
      writer.writeEndDocument()
    }

    // no merge operation for avgLen, .maxLen, histogram, or version
    writer.writeEndDocument()
  }




  def getPartId(table : CatalogTable, partitionSpec : CatalogTypes.TablePartitionSpec): String = {
    var partId = ""
    table.partitionSchema.foreach { partitionColumn =>
      if (partitionSpec.contains(partitionColumn.name)) {
        val partitionVal = getPartitionVal(partitionColumn, partitionSpec)
        partId = partId + "/" +  partitionColumn.name + "=" + partitionVal
      }
    }

    partId
  }

  def getFileId(table : CatalogTable, file : CatalogTableFile): String = {
    var fileId = ""
    table.partitionSchema.foreach { partitionColumn =>
      val partitionVal = getPartitionVal(partitionColumn, file.partitionValues)
      fileId = fileId + "/" +  partitionColumn.name + "=" + partitionVal
    }
    val filePath = file.storage.locationUri.get.toString
    fileId + filePath.slice(filePath.lastIndexOf('/'), filePath.size)
  }

  // helper function to encode int into 12 char long string
  def getPartitionVal(partitionColumn: StructField, partitionSpec : Map[String, String]): String = {
    val partitionValOption = partitionSpec.get(partitionColumn.name)
    if (partitionColumn.dataType.isInstanceOf[IntegralType]) {
      if (partitionValOption.isDefined) {
        "%012d".format(partitionValOption.get.toLong)
      }
      else {
        "DEFAULT"
      }
    }
    else {
      partitionValOption.getOrElse("DEFAULT")
    }
  }

  def appendBson(writer : BsonBinaryWriter, storage : CatalogStorageFormat) : Unit = {
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

  def appendBson(writer : BsonBinaryWriter, map : Map[String, String]) : Unit = {
    writer.writeStartDocument()
    map.foreach{ case (key, value) =>
      writer.writeString(key, value)
    }
    writer.writeEndDocument()
  }

  // first type for statistics, which converts min and max to original data type
  def appendBson(writer : BsonBinaryWriter, table : CatalogTable,
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

  def appendBson(writer : BsonBinaryWriter, dataType: DataType, colStat : CatalogColumnStat):
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

  def toCatalogDatabase(bsonDocument : BsonDocument) : CatalogDatabase = {
    val name = bsonDocument.get("name").asString().getValue
    val description = bsonDocument.get("description").asString.getValue
    val locationUri = new URI(bsonDocument.get("locationUri").asString.getValue)
    val properties = toMap(bsonDocument.get("properties").asDocument())

    CatalogDatabase(name, description, locationUri, properties)
  }

  def toCatalogTable(bsonDocument : BsonDocument) : CatalogTable = {
    val identifier = toTableIdentifier(bsonDocument.get("identifier").asDocument())
    val tableType = toCatalogTableType(bsonDocument.get("tableType").asDocument())
    val storage = toCatalogStorageFormat(bsonDocument.get("storage").asDocument())
    val schema = toStructType(bsonDocument.get("schema").asDocument())
    val provider = {
      val providerBson = bsonDocument.get("provider")
      if (providerBson != null) {
        Some(providerBson.asString().getValue)
      }
      else {
        None
      }
    }
    val partitionColumnNames = toSeq(bsonDocument.get("partitionColumnNames").asArray())
    val bucketSpec = None
    val owner = bsonDocument.get("owner").asString().getValue
    val createTime = bsonDocument.get("createTime").asNumber().longValue()
    val lastAccessTime = bsonDocument.get("lastAccessTime").asNumber().longValue()
    val createVersion = bsonDocument.get("createVersion").asString().getValue
    val properties = toMap(bsonDocument.get("properties").asDocument())
    val stats : Option[CatalogStatistics] = {
      val statsBson = bsonDocument.get("stats")
      if (statsBson != null) {
        Some(toCatalogStatistics(statsBson.asDocument()))
      }
      else {
        None
      }
    }
    val viewText = {
      val viewTextBson = bsonDocument.get("viewText")
      if (viewTextBson != null) {
        Some(viewTextBson.asString().getValue)
      }
      else {
        None
      }
    }
    val comment = {
      val commentBson = bsonDocument.get("comment")
      if (commentBson != null) {
        Some(commentBson.asString().getValue)
      }
      else {
        None
      }
    }
    val unsupportedFeatures = toSeq(bsonDocument.get("unsupportedFeatures").asArray())
    val tracksPartitionsInCatalog = bsonDocument.get("tracksPartitionsInCatalog").asBoolean()
      .getValue
    val schemaPreservesCase = bsonDocument.get("schemaPreservesCase").asBoolean().getValue
    val ignoredProperties = toMap(bsonDocument.get("ignoredProperties").asDocument())
    val viewOriginalText = {
      val viewOriginalTextBson = bsonDocument.get("viewOriginalText")
      if (viewOriginalTextBson != null) {
        Some(viewOriginalTextBson.asString().getValue)
      }
      else {
        None
      }
    }
    CatalogTable(identifier, tableType, storage, schema, provider, partitionColumnNames,
      bucketSpec, owner, createTime, lastAccessTime, createVersion, properties, stats, viewText,
      comment, unsupportedFeatures, tracksPartitionsInCatalog, schemaPreservesCase,
      ignoredProperties, viewOriginalText)
  }

  // deserializer for CatalogTablePartition
  def toCatalogTablePartition(bsonDocument : BsonDocument) : CatalogTablePartition = {
    val spec = toMap(bsonDocument.get("spec").asDocument())
    val storage = toCatalogStorageFormat(bsonDocument.get("storage").asDocument())
    val parameters = toMap(bsonDocument.get("parameters").asDocument())
    val createTime = bsonDocument.get("createTime").asNumber().longValue()
    val lastAccessTime = bsonDocument.get("lastAccessTime").asNumber().longValue()
    val stats : Option[CatalogStatistics] = {
      val statsBson = bsonDocument.get("stats")
      if (statsBson != null) {
        Some(toCatalogStatistics(statsBson.asDocument()))
      }
      else {
        None
      }
    }
    CatalogTablePartition(spec, storage, parameters, createTime, lastAccessTime, stats)
  }

  def toFileStatus(bsonDocument : BsonDocument) : FileStatus = {
    // scalastyle:off pathfromuri
    val path = new Path(new URI(bsonDocument.get("storage").asDocument()
      .get("locationUri").asString().getValue()))
    // scalastyle:on pathfromuri
    val size = bsonDocument.get("size").asNumber().longValue()
    val modificationTime = bsonDocument.get("modificationTime").asNumber().longValue()

    new FileStatus(
      /* length */ size,
      /* isDir */ false,
      /* blockReplication */ 0,
      /* blockSize */ 1,
      /* modificationTime */ modificationTime,
      path)
  }

  // deserializer for CatalogTableFile
  def toCatalogTableFile(bsonDocument : BsonDocument) : CatalogTableFile = {
    val storage = toCatalogStorageFormat(bsonDocument.get("storage").asDocument())
    val partitionValues = toMap(bsonDocument.get("partitionValues").asDocument())
    val size = bsonDocument.get("size").asNumber().longValue()
    val modificationTime = bsonDocument.get("modificationTime").asNumber().longValue()
    val stats : Option[CatalogStatistics] = {
      val statsBson = bsonDocument.get("stats")
      if (statsBson != null) {
        Some(toCatalogStatistics(statsBson.asDocument()))
      }
      else {
        None
      }
    }
    val tags = toMap(bsonDocument.get("tags").asDocument())

    CatalogTableFile(storage, partitionValues, size, modificationTime, stats, tags)

  }

  private def toTableIdentifier(bsonDocument: BsonDocument) : TableIdentifier = {
    val table = bsonDocument.get("table").asString().getValue
    val database = {
      val databaseBson = bsonDocument.get("database")
      if (databaseBson != null) {
        Some(databaseBson.asString().getValue)
      }
      else {
        None
      }
    }
    val catalog = {
      val catalogBson = bsonDocument.get("catalog")
      if (catalogBson != null) {
        Some(catalogBson.asString().getValue)
      }
      else {
        None
      }
    }
    TableIdentifier(table, database, catalog)
  }

  private def toCatalogTableType(bsonDocument : BsonDocument) : CatalogTableType = {
    val name = bsonDocument.get("name").asString().getValue
    name match {
      case "EXTERNAL" => CatalogTableType.EXTERNAL
      case "MANAGED" => CatalogTableType.MANAGED
      case "VIEW" => CatalogTableType.VIEW
    }
  }

  private def toStructType(bsonDocument: BsonDocument): StructType = {
    DataType.fromJson(bsonDocument.toJson(jsonWriterSetting)).asInstanceOf[StructType]
  }

  private def toSeq(bsonArray : BsonArray) : Seq[String] = {
    val seq = ArrayBuffer[String]()
    bsonArray.forEach { elem =>
      seq.append(elem.asString().getValue)
    }
    seq
  }

  def toCatalogStorageFormat(bsonDocument : BsonDocument) : CatalogStorageFormat = {
    val locationUri: Option[URI] = {
      val locationUriBson = bsonDocument.get("locationUri")
      if (locationUriBson != null) {
        Some(new URI(locationUriBson.asString().getValue()))
      }
      else {
        None
      }
    }

    val inputFormat: Option[String] = {
      val inputFormatBson = bsonDocument.get("inputFormat")
      if (inputFormatBson != null) {
        Some(inputFormatBson.asString().getValue())
      }
      else {
        None
      }
    }

    val outputFormat: Option[String] = {
      val outputFormatBson = bsonDocument.get("outputFormat")
      if (outputFormatBson != null) {
        Some(outputFormatBson.asString().getValue())
      }
      else {
        None
      }
    }

    val serde: Option[String] = {
      val serdeBson = bsonDocument.get("serde")
      if (serdeBson != null) {
        Some(serdeBson.asString().getValue())
      }
      else {
        None
      }
    }

    val compressed = bsonDocument.get("compressed").asBoolean().getValue()
    val propertiesBson = toMap(bsonDocument.get("properties").asDocument())

    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serde,
      compressed, propertiesBson)
  }

  def toMap(bsonDocument : BsonDocument) : immutable.Map[String, String] = {
    val mutableMap : mutable.Map[String, String] = mutable.Map.empty
    bsonDocument.forEach{ (key, value) =>
      mutableMap.put(key, value.asString().getValue())
    }
    mutableMap.toMap
  }

  def toCatalogStatistics(bsonDocument : BsonDocument): CatalogStatistics = {
    val sizeInBytes = bsonDocument.get("sizeInBytes").asNumber().longValue()
    val rowCount = {
      val rowCountBson = bsonDocument.get("rowCount")
      if (rowCountBson != null) {
        Some(BigInt(rowCountBson.asNumber().longValue()))
      }
      else {
        None
      }
    }
    val colStatsBson = bsonDocument.get("colStats").asDocument()
    val colStats : mutable.Map[String, CatalogColumnStat] = mutable.Map.empty
    colStatsBson.entrySet().forEach{ entry =>
      colStats.put(entry.getKey, toCatalogColumnStat(entry.getValue.asDocument()))
    }

    CatalogStatistics(BigInt(sizeInBytes), rowCount, colStats.toMap)
  }

  private def bsonValToString(bsonVal : BsonValue) : Option[String] = {
    val bsonType = bsonVal.getBsonType
    bsonType match {
      case BsonType.STRING => Some(bsonVal.asString().getValue)
      case BsonType.INT32 => Some(bsonVal.asInt32().toString)
      case BsonType.INT64 => Some(bsonVal.asInt64().toString)
      case BsonType.BOOLEAN => Some(bsonVal.asBoolean().toString)
      case BsonType.DOUBLE => Some(bsonVal.asDouble().toString)
      case _ => None
    }
  }

  def toCatalogColumnStat(bsonDocument : BsonDocument) : CatalogColumnStat = {
    val distinctCount = {
      val distinctCountBson = bsonDocument.get("distinctCount")
      if (distinctCountBson != null) {
        Some(BigInt(distinctCountBson.asNumber().longValue()))
      }
      else {
        None
      }
    }
    val min = {
      val minBson = bsonDocument.get("min")
      if (minBson != null) {
        bsonValToString(minBson)
      }
      else {
        None
      }
    }
    val max = {
      val maxBson = bsonDocument.get("max")
      if (maxBson != null) {
        bsonValToString(maxBson)
      }
      else {
        None
      }
    }
    val nullCount = {
      val nullCountBson = bsonDocument.get("nullCount")
      if (nullCountBson != null) {
        Some(BigInt(nullCountBson.asNumber().longValue()))
      }
      else {
        None
      }
    }
    val avgLen = {
      val avgLenBson = bsonDocument.get("avgLen")
      if (avgLenBson != null) {
        Some(avgLenBson.asNumber().longValue())
      }
      else {
        None
      }
    }
    val maxLen = {
      val maxLenBson = bsonDocument.get("maxLen")
      if (maxLenBson != null) {
        Some(maxLenBson.asNumber().longValue())
      }
      else {
        None
      }
    }
    // we do not support histograms
    val version = bsonDocument.get("version").asNumber().intValue()

    CatalogColumnStat(distinctCount, min, max, nullCount, avgLen, maxLen, None, version)

  }

  // create empty statistics with default base values so it is mergeable
  def emptyStats(table : CatalogTable) : CatalogStatistics = {
    val sizeInBytes = BigInt(0)
    val rowCount = Some(BigInt(0))
    val colStats = scala.collection.mutable.Map.empty[String, CatalogColumnStat]
    val schema = table.schema
    // exclude partition columns
    for (i <- 0 until schema.length - table.partitionColumnNames.length) {
      colStats.put(schema(i).name, emptyColStat(schema(i).dataType))
    }

    CatalogStatistics(sizeInBytes, rowCount, colStats.toMap)

  }

  private def emptyColStat(dataType : DataType) : CatalogColumnStat = {
    val min = {
      dataType match {
        case ByteType => Some(Byte.MinValue.toString)
        case ShortType => Some(Short.MinValue.toString)
        case IntegerType => Some(Int.MinValue.toString)
        case LongType => Some(Long.MinValue.toString)
        case BooleanType => Some("false")
        case FloatType => Some(Float.MinValue.toString)
        case DoubleType => Some(Double.MinValue.toString)
        case StringType => Some("")
        case DateType => Some("0000-01-01")
        // other types are currently not supported
        case _ => None
      }
    }
    val max = {
      dataType match {
        case ByteType => Some(Byte.MaxValue.toString)
        case ShortType => Some(Short.MaxValue.toString)
        case IntegerType => Some(Int.MaxValue.toString)
        case LongType => Some(Long.MaxValue.toString)
        case BooleanType => Some("true")
        case FloatType => Some(Float.MaxValue.toString)
        case DoubleType => Some(Double.MaxValue.toString)
        // This should work for most unicode
        // scalastyle:off
        case StringType => Some("\uFFFF")
        // scalastyle:on
        case DateType => Some("9999-12-31")
        // other types are currently not supported
        case _ => None
      }
    }
    val nullCount = Some(BigInt(0))
    val version = 1

    CatalogColumnStat(None, min, max, nullCount, None, None, None, version)

  }
}

// Proxy for keeping track of txn state
private[spark] class TreeTxn(val txnMode : TxnMode,
                             val txnId : Option[Long] = None,
                             val vid : Option[Long] = None,
                             val commitRequest : Option[CommitRequest.Builder] = None) {
  private var aborted : Boolean = false

  def setAbort(abort : Boolean): Unit = {
    aborted = abort
  }

  def isOK(): Boolean = {
    !aborted
  }

}

private[spark] class TreeExternalCatalog(address : String = "localhost:9876",
                                         channelOption : Option[ManagedChannel] = None)
  extends Logging {
  private implicit val formats = Serialization.formats(NoTypeHints) + new HiveURISerializer +
    new HiveDataTypeSerializer + new HiveMetadataSerializer + new HiveStructTypeSerializer
  private val addressPort = address.split(":")
  private val channel = channelOption.getOrElse(
    ManagedChannelBuilder.forAddress(addressPort(0), addressPort(1).toInt)
    .usePlaintext()
    .asInstanceOf[ManagedChannelBuilder[_]].build())

  private val catalogStub: GRPCCatalogGrpc.GRPCCatalogBlockingStub =
    GRPCCatalogGrpc.newBlockingStub(channel)
  val json_writer_setting : JsonWriterSettings = JsonWriterSettings.builder().
    outputMode(JsonMode.RELAXED).build()

  private class BufIterator(buf : Array[Byte]) {
    private var elem_size_ = 0
    private var data_idx_ = 0
    private var next_ = 0
    private var valid_ = false

    if (buf.length >= Integer.BYTES) {
      elem_size_ = ByteBuffer.wrap(buf.slice(0, Integer.BYTES)).
        order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
      data_idx_ = Integer.BYTES
      next_ = Integer.BYTES + elem_size_
      if (next_ <= buf.size) {
        valid_ = true
      }
    }

    def next() : Boolean = {
      if (!valid_) {
        false
      }
      else if (next_ + Integer.BYTES >= buf.length) {
        valid_ = false
        false
      }
      else {
        elem_size_ = ByteBuffer.wrap(buf.slice(next_, next_ + Integer.BYTES)).
          order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt()
        data_idx_ = next_ + Integer.BYTES
        next_ = data_idx_ + elem_size_

        if (next_ > buf.length) {
          valid_ = false
          false
        }
        else {
          valid_ = true
          true
        }
      }

    }

    def valid() : Boolean = {
      valid_
    }

    def dataIdx() : Int = {
      data_idx_
    }

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

  private def constructBinaryPred(field: String, op : ExprOpType, const_str : String)
      : Predicate = {
    Predicate.newBuilder().setExprNode(constructBinaryExpr(field, op, const_str)).build()
  }

  private def constructBinaryExpr(field: String, op : ExprOpType, const_str : String)
  : ExprNode = {
    ExprNode.newBuilder()
      .setExprOp(ExprOp.newBuilder().setLeft(ExprNode.newBuilder()
      .setExprFieldRef(ExprFieldRef.newBuilder().addFieldRefs(field)))
      .setOpType(op).setRight(ExprNode.newBuilder()
      .setExprConst(ExprConst.newBuilder().setConstType(ExprConstType.EXPR_CONST_TYPE_STRING)
      .setStringVal(const_str)))).build()
  }

  private def quoteStringLiteral(str: String): String = {
    if (!str.contains("\"")) {
      s""""$str""""
    } else if (!str.contains("'")) {
      s"""'$str'"""
    } else {
      s"""'$str'"""
    }
  }

  private def convertFilters(table: CatalogTable, filters: Seq[Expression]): PathExpr.Builder = {
    lazy val dateFormatter = DateFormatter()

    /**
     * An extractor that matches all binary comparison operators except null-safe equality.
     *
     * Null-safe equality is not supported by Hive metastore partition predicate pushdown
     */
    object SpecialBinaryComparison {
      def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
        case _: EqualNullSafe => None
        case _ => Some((e.left, e.right))
      }
    }

    object ExtractableLiteral {
      def unapply(expr: Expression): Option[String] = expr match {
        case Literal(null, _) => None // `null`s can be cast as other types; we want to avoid NPEs.
        case Literal(value, _: IntegralType) => Some(value.toString)
        case Literal(value, _: StringType) => Some(value.toString)
        case Literal(value, _: DateType) =>
          Some(dateFormatter.format(value.asInstanceOf[Int]))
        case _ => None
      }
    }

    object ConstructableLiteral {
      def unapply(expr: Expression): Option[Literal] = expr match {
        case Literal(null, _) => None // `null`s can be cast as other types; we want to avoid NPEs.
        case Literal(value, int_type: IntegralType) => Some(Literal(value, int_type))
        case Literal(value, str_type: StringType) => Some(Literal(value, str_type))
        case Literal(value, date_type: DateType) => Some(Literal(value, date_type))
        case _ => None
      }
    }

    object ExtractableLiterals {
      def unapply(exprs: Seq[Expression]): Option[Seq[String]] = {
        // SPARK-24879: The Hive metastore filter parser does not support "null", but we still want
        // to push down as many predicates as we can while still maintaining correctness.
        // In SQL, the `IN` expression evaluates as follows:
        //  > `1 in (2, NULL)` -> NULL
        //  > `1 in (1, NULL)` -> true
        //  > `1 in (2)` -> false
        // Since Hive metastore filters are NULL-intolerant binary operations joined only by
        // `AND` and `OR`, we can treat `NULL` as `false` and thus rewrite `1 in (2, NULL)` as
        // `1 in (2)`.
        // If the Hive metastore begins supporting NULL-tolerant predicates and Spark starts
        // pushing down these predicates, then this optimization will become incorrect and need
        // to be changed.
        val extractables = exprs
          .filter {
            case Literal(null, _) => false
            case _ => true
          }.map(ExtractableLiteral.unapply)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object ConstructableLiterals {
      def unapply(exprs: Seq[Expression]): Option[Seq[Literal]] = {
        val constructables = exprs
          .filter {
            case Literal(null, _) => false
            case _ => true
          }.map(ConstructableLiteral.unapply)
        if (constructables.nonEmpty && constructables.forall(_.isDefined)) {
          Some(constructables.map(_.get))
        } else {
          None
        }
      }
    }


    object ExtractableValues {
      private lazy val valueToLiteralString: PartialFunction[Any, String] = {
        case value: Byte => value.toString
        case value: Short => value.toString
        case value: Int => value.toString
        case value: Long => value.toString
        case value: UTF8String => quoteStringLiteral(value.toString)
      }

      def unapply(values: Set[Any]): Option[Seq[String]] = {
        val extractables = values.filter(_ != null).toSeq.map(valueToLiteralString.lift)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object ConstructableValues {
      private lazy val valueToLiteral: PartialFunction[Any, Literal] = {
        case value: Byte => Literal(value, ByteType)
        case value: Short => Literal(value, ShortType)
        case value: Int => Literal(value, IntegerType)
        case value: Long => Literal(value, LongType)
        case value: UTF8String => Literal(value.toString, StringType)
      }

      def unapply(values: Set[Any]): Option[Seq[Literal]] = {
        val constructables = values.filter(_ != null).toSeq.map(valueToLiteral.lift)
        if (constructables.nonEmpty && constructables.forall(_.isDefined)) {
          Some(constructables.map(_.get))
        } else {
          None
        }
      }
    }

//    object ExtractableDateValues {
//      private lazy val valueToLiteralString: PartialFunction[Any, String] = {
//        case value: Int => dateFormatter.format(value)
//      }
//
//      def unapply(values: Set[Any]): Option[Seq[String]] = {
//        val extractables = values.filter(_ != null).toSeq.map(valueToLiteralString.lift)
//        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
//          Some(extractables.map(_.get))
//        } else {
//          None
//        }
//      }
//    }
//
//    object SupportedAttribute {
//      def unapply(attr: Attribute): Option[String] = {
//        if (attr.dataType.isInstanceOf[IntegralType] ||
//          attr.dataType == StringType || attr.dataType == DateType) {
//          Some(attr.name)
//        } else {
//          None
//        }
//      }
//    }
//
    def convertInToOr(attr: Attribute, values: Seq[Literal]): Expression = {
      val false_literal = Literal(false, BooleanType)
      values.foldLeft(Or(false_literal, false_literal))((or_expr, value)
          => Or(or_expr, EqualTo(attr, value)))
    }

    def convertNotInToAnd(attr: Attribute, values: Seq[Literal]): Expression = {
      val true_literal = Literal(true, BooleanType)
      values.foldLeft(And(true_literal, true_literal))((and_expr, value)
          => And(and_expr, Not(EqualTo(attr, value))))
    }

    def hasNullLiteral(list: Seq[Expression]): Boolean = list.exists {
      case Literal(null, _) => true
      case _ => false
    }


    object ExtractAttribute {
      @scala.annotation.tailrec
      def unapply(expr: Expression): Option[Attribute] = {
        expr match {
          case attr: Attribute => Some(attr)
          case Cast(child @ IntegralType(), dt: IntegralType, _, _)
            if Cast.canUpCast(child.dataType.asInstanceOf[AtomicType], dt) => unapply(child)
          case _ => None
        }
      }
    }

    def convert(col_name: String, expr: Expression): Option[ExprNode] = expr match {
      case Not(In(_, list)) if hasNullLiteral(list) => None

      case Not(InSet(_, list)) if list.contains(null) => None

      case In(ExtractAttribute(attr), ConstructableLiterals(values)) =>
        convert(col_name, convertInToOr(attr, values))


      case Not(In(ExtractAttribute(attr), ConstructableLiterals(values))) =>
        convert(col_name, convertNotInToAnd(attr, values))

      case InSet(ExtractAttribute(attr), ConstructableValues(values)) =>
        convert(col_name, convertInToOr(attr, values))

      case Not(InSet(ExtractAttribute(attr), ConstructableValues(values))) =>
        convert(col_name, convertNotInToAnd(attr, values))

      case op @ SpecialBinaryComparison(ExtractAttribute(attr), ExtractableLiteral(value)) =>
        if (attr.name == col_name) {
          val expr_op_type = op match {
            case EqualTo(_, _) => ExprOpType.EXPR_OP_TYPE_EQUALS
            case EqualNullSafe(_, _) => ExprOpType.EXPR_OP_TYPE_EQUALS
            case LessThan(_, _) => ExprOpType.EXPR_OP_TYPE_LESS
            case LessThanOrEqual(_, _) => ExprOpType.EXPR_OP_TYPE_LESS_EQUALS
            case GreaterThan(_, _) => ExprOpType.EXPR_OP_TYPE_GREATER
            case GreaterThanOrEqual(_, _) => ExprOpType.EXPR_OP_TYPE_GREATER_EQUALS
          }
          Some(constructBinaryExpr("obj_id", expr_op_type, value))
        }
        else {
          None
        }

      case op @ SpecialBinaryComparison(ExtractableLiteral(value), ExtractAttribute(attr)) =>
        // flip the operators as attribute always goes to the left in ExprNode
        if (attr.name == col_name) {
          val expr_op_type = op match {
            case EqualTo(_, _) => ExprOpType.EXPR_OP_TYPE_EQUALS
            case EqualNullSafe(_, _) => ExprOpType.EXPR_OP_TYPE_EQUALS
            case LessThan(_, _) => ExprOpType.EXPR_OP_TYPE_GREATER
            case LessThanOrEqual(_, _) => ExprOpType.EXPR_OP_TYPE_GREATER_EQUALS
            case GreaterThan(_, _) => ExprOpType.EXPR_OP_TYPE_LESS
            case GreaterThanOrEqual(_, _) => ExprOpType.EXPR_OP_TYPE_LESS_EQUALS
          }
          Some(constructBinaryExpr("obj_id", expr_op_type, value))
        }
        else {
          None
        }

      case And(expr1, expr2) =>
        val converted_expr1 = convert(col_name, expr1)
        val converted_expr2 = convert(col_name, expr2)
        if (converted_expr1.isEmpty && converted_expr2.isEmpty) {
          None
        } else {
          val and_builder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
          if (!converted_expr1.isEmpty) {
            and_builder.addArgs(converted_expr1.get)
          }
          if (!converted_expr2.isEmpty) {
            and_builder.addArgs(converted_expr2.get)
          }

          Some(ExprNode.newBuilder().setExprBool(and_builder).build())
        }

      case Or(expr1, expr2) =>
        val converted_expr1 = convert(col_name, expr1)
        val converted_expr2 = convert(col_name, expr2)
        if (converted_expr1.isEmpty && converted_expr2.isEmpty) {
          None
        } else {
          val or_builder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_OR)
          if (!converted_expr1.isEmpty) {
            or_builder.addArgs(converted_expr1.get)
          }
          if (!converted_expr2.isEmpty) {
            or_builder.addArgs(converted_expr2.get)
          }

          Some(ExprNode.newBuilder().setExprBool(or_builder).build())
        }

      case Not(EqualTo(
      ExtractAttribute(attr), ExtractableLiteral(value))) =>
        Some(constructBinaryExpr(attr.name, ExprOpType.EXPR_OP_TYPE_NOT_EQUALS, value))

      case Not(EqualTo(
      ExtractableLiteral(value), ExtractAttribute(attr))) =>
        Some(constructBinaryExpr(attr.name, ExprOpType.EXPR_OP_TYPE_NOT_EQUALS, value))

      case _ => None
    }
    val pathExprBuilder = PathExpr.newBuilder()

    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)

    pathExprBuilder.addPreds(dbPred).addPreds(tablePred)

    val partTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")
    table.partitionColumnNames.foreach{ col_name =>
      val andBuilder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
      filters.map(convert(col_name, _)).filter(!_.isEmpty)
        .foreach(expr => andBuilder.addArgs(expr.get))
      if (andBuilder.getArgsCount == 0) {
        pathExprBuilder.addPreds(Predicate.newBuilder().setExprNode(partTypeExpr))
      }
      else {
        pathExprBuilder.addPreds(Predicate.newBuilder()
            .setExprNode(ExprNode.newBuilder().setExprBool(andBuilder.addArgs(partTypeExpr))))
      }
    }

    pathExprBuilder
  }

  private def constructDbPred(db: String) : Predicate = {
    val dbOidExpr = constructBinaryExpr("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, db)
    val dbTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "database")
    Predicate.newBuilder().setExprNode(ExprNode.newBuilder()
      .setExprBool(ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
        .addArgs(dbOidExpr).addArgs(dbTypeExpr))).build()
  }

  private def constructTablePred(table : String) : Predicate = {
    val tableOidExpr = constructBinaryExpr("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, table)
    val tableTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "table")
    Predicate.newBuilder().setExprNode(ExprNode.newBuilder()
      .setExprBool(ExprBool.newBuilder().addArgs(tableOidExpr).addArgs(tableTypeExpr)
        .setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND))).build()
  }

  private def setTxnId(queryRequest : ExecuteQueryRequest.Builder, txn : Option[TreeTxn]) = {
    if (txn.isDefined) {
      if (txn.get.txnMode == TxnMode.TXN_MODE_READ_ONLY)  {
        queryRequest.setVid(txn.get.vid.get)
      }
      else if (txn.get.txnMode == TxnMode.TXN_MODE_READ_WRITE) {
        queryRequest.setTxnId(txn.get.txnId.get)
      }
    }
  }

  private def toCatalogTablePartitions(queryResponses: java.util.Iterator[ExecuteQueryResponse],
                                       txn : Option[TreeTxn])
      : Seq[CatalogTablePartition] = {
    val partitions = ArrayBuffer[CatalogTablePartition]()
    var lastResponse : Option[ExecuteQueryResponse] = None
    queryResponses.forEachRemaining(response => {
      lastResponse = Some(response)
      val responseBuf = response.getObjList().toByteArray()
      val bufIter = new BufIterator(responseBuf)
      while (bufIter.valid()) {
        val partBson = new RawBsonDocument(responseBuf, bufIter.dataIdx(),
          responseBuf.size - bufIter.dataIdx())
        partitions += TreeSerde.toCatalogTablePartition(partBson)
        bufIter.next()
      }
    })
    if (txn.isDefined && lastResponse.isDefined && lastResponse.get.hasAbort) {
      txn.get.setAbort(lastResponse.get.getAbort)
      commit(txn.get)
    }
    partitions
  }

  private def toCatalogTableFiles(queryResponses: java.util.Iterator[ExecuteQueryResponse],
                                  txn : Option[TreeTxn])
  : Seq[CatalogTableFile] = {
    val files = ArrayBuffer[CatalogTableFile]()
    var lastResponse : Option[ExecuteQueryResponse] = None
    queryResponses.forEachRemaining(response => {
      lastResponse = Some(response)
      val responseBuf = response.getObjList().toByteArray()
      val bufIter = new BufIterator(responseBuf)
      while (bufIter.valid()) {
        val fileBson = new RawBsonDocument(responseBuf, bufIter.dataIdx(),
          responseBuf.size - bufIter.dataIdx())
        files += TreeSerde.toCatalogTableFile(fileBson)
        bufIter.next()
      }
    })
    if (txn.isDefined && lastResponse.isDefined && lastResponse.get.hasAbort) {
      txn.get.setAbort(lastResponse.get.getAbort)
      commit(txn.get)
    }
    files
  }

  private def toFileStatuses(queryResponses: java.util.Iterator[ExecuteQueryResponse],
                                  txn : Option[TreeTxn])
  : Seq[FileStatus] = {
    val files = ArrayBuffer[FileStatus]()
    var lastResponse : Option[ExecuteQueryResponse] = None
    queryResponses.forEachRemaining(response => {
      lastResponse = Some(response)
      val responseBuf = response.getObjList().toByteArray()
      val bufIter = new BufIterator(responseBuf)
      while (bufIter.valid()) {
        val fileBson = new RawBsonDocument(responseBuf, bufIter.dataIdx(),
          responseBuf.size - bufIter.dataIdx())
        files += TreeSerde.toFileStatus(fileBson)
        bufIter.next()
      }
    })
    if (txn.isDefined && lastResponse.isDefined && lastResponse.get.hasAbort) {
      txn.get.setAbort(lastResponse.get.getAbort)
      commit(txn.get)
    }
    files
  }

  // start a new txn. returns Some(Txn) if successful
  def startTransaction(txnMode : TxnMode) : Option[TreeTxn] = {
    val startTxnResponse = catalogStub.startTxn(StartTxnRequest.newBuilder()
        .setTxnMode(txnMode).build)
    if (startTxnResponse.getSuccess && txnMode == TxnMode.TXN_MODE_READ_ONLY) {
      Some(new TreeTxn(txnMode, None, Some(startTxnResponse.getVid), None))
    }
    else if (startTxnResponse.getSuccess && txnMode == TxnMode.TXN_MODE_READ_WRITE) {
      val txnId = startTxnResponse.getTxnId
      Some(new TreeTxn(txnMode, Some(txnId), None, Some(CommitRequest.newBuilder()
        .setTxnId(txnId))))
    }
    else {
      None
    }
  }

  // commits the given txn and returns true if successful
  def commit(txn: TreeTxn) : Boolean = {
    if (txn.txnMode == TxnMode.TXN_MODE_READ_ONLY && txn.isOK()) {
      true
    }
    else if (txn.txnMode == TxnMode.TXN_MODE_READ_WRITE && txn.commitRequest.isDefined) {
      // abort if txn is not ok
      if (!txn.isOK()) {
        txn.commitRequest.get.setAbort(true)
      }

      val commitResponse = catalogStub.commit(txn.commitRequest.get.build)
      commitResponse.getSuccess
    }
    else {
      false
    }

  }

  def getDatabase(db: String, txn : Option[TreeTxn] = None): CatalogDatabase = {
    val dbPred = constructDbPred(db)
    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(
        PathExpr.newBuilder().addPreds(dbPred))
        .setBaseOnly(true).setReturnType(1)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    if (queryResponses.hasNext) {
      val firstResponse = queryResponses.next()
      var lastResponse = firstResponse
      while (queryResponses.hasNext) {
        lastResponse = queryResponses.next()
      }
      if (txn.isDefined && lastResponse.hasAbort) {
        txn.get.setAbort(lastResponse.getAbort)
        commit(txn.get)
      }

      val responseBuf = firstResponse.getObjList().toByteArray()
      val bufIter = new BufIterator(responseBuf)
      if (bufIter.valid()) {
        val DBBson = new RawBsonDocument(responseBuf, bufIter.dataIdx(),
          responseBuf.size - bufIter.dataIdx())
        TreeSerde.toCatalogDatabase(DBBson)
      }
      else {
        null
      }
    }
    else {
      null
    }
  }

  def getTable(db: String, table: String, txn : Option[TreeTxn] = None): CatalogTable = {
    val dbPred = constructDbPred(db)
    val tablePred = constructTablePred(table)

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder().
        addPreds(dbPred).addPreds(tablePred).build()).setBaseOnly(true).setReturnType(1)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    if (queryResponses.hasNext) {
      val firstResponse = queryResponses.next()
      var lastResponse = firstResponse
      while (queryResponses.hasNext) {
        lastResponse = queryResponses.next()
      }
      // if part of a txn session, check if txn has been aborted
      if (txn.isDefined && lastResponse.hasAbort) {
        txn.get.setAbort(lastResponse.getAbort)
        // commit to deallocate resources on the server side
        commit(txn.get)
      }

      val responseBuf = firstResponse.getObjList().toByteArray()
      val bufIter = new BufIterator(responseBuf)
      if (bufIter.valid()) {
        val tableBson = new RawBsonDocument(responseBuf, bufIter.dataIdx(),
          responseBuf.size - bufIter.dataIdx())
        TreeSerde.toCatalogTable(tableBson)
      }
      else {
        null
      }
    }
    else {
      null
    }
  }

  def listTables(db: String, txn : Option[TreeTxn] = None): Seq[String] = {
    val dbPred = constructDbPred(db)
    val tablesPred = constructBinaryPred("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "table")

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder()
        .addPreds(dbPred).addPreds(tablesPred).build()).setBaseOnly(true)
        .setReturnType(1)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    val tables = ArrayBuffer[String]()
    var lastResponse : Option[ExecuteQueryResponse] = None
    queryResponses.forEachRemaining { response =>
      lastResponse = Some(response)
      val responseBuf = response.getObjList().toByteArray()
      val bufIter = new BufIterator(responseBuf)
      while (bufIter.valid()) {
        val table_bson = new RawBsonDocument(responseBuf, bufIter.dataIdx(),
          responseBuf.size - bufIter.dataIdx())
        tables += table_bson.get("identifier").asInstanceOf[RawBsonDocument].get("table").
          asString().getValue()
        bufIter.next()
      }
    }
    if (txn.isDefined && lastResponse.isDefined && lastResponse.get.hasAbort) {
      txn.get.setAbort(lastResponse.get.getAbort)
      commit(txn.get)
    }
    tables
  }

  def listPartitions(db: String, table: String, txn : Option[TreeTxn]):
      Seq[CatalogTablePartition] = {
    val tableObj = getTable(db, table, txn)
    if (tableObj != null) {
      listPartitions(tableObj, txn)
    }
    else {
      ArrayBuffer[CatalogTablePartition]()
    }
  }

  def listPartitions(table: CatalogTable, txn : Option[TreeTxn]):
      Seq[CatalogTablePartition] = {
    if (table.partitionColumnNames.isEmpty) {
      ArrayBuffer[CatalogTablePartition]()
    }
    else {
      val dbPred = constructDbPred(table.identifier.database.get)
      val tablePred = constructTablePred(table.identifier.table)

      val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)

      val partPred = constructBinaryPred("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")

      table.partitionColumnNames.foreach { _ =>
        pathExprBuilder.addPreds(partPred)
      }

      val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(
        pathExprBuilder.build()).setBaseOnly(true).setReturnType(1)
      setTxnId(queryRequest, txn)

      val queryResponses = catalogStub.executeQuery(queryRequest.build())
      toCatalogTablePartitions(queryResponses, txn)
    }
  }

  def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression],
                             txn : Option[TreeTxn]):
      Seq[CatalogTablePartition] = {
    val tableObj = getTable(db, table, txn)
    if (tableObj != null) {
      listPartitionsByFilter(tableObj, predicates, txn)
    }
    else {
      ArrayBuffer[CatalogTablePartition]()
    }
  }

  def listPartitionsByFilter(table: CatalogTable, predicates: Seq[Expression],
                             txn : Option[TreeTxn]):
      Seq[CatalogTablePartition] = {
    val pathExprBuilder = convertFilters(table, predicates)
    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(
      pathExprBuilder.build()).setBaseOnly(true).setReturnType(1)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    toCatalogTablePartitions(queryResponses, txn)
  }

  def getPartition(table: CatalogTable, spec: Map[String, String], txn : Option[TreeTxn] = None):
      Seq[CatalogTablePartition] = {
    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)
    val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)

    val partTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")
    table.partitionSchema.foreach { partitionColumn =>
      val partitionVal = TreeSerde.getPartitionVal(partitionColumn, spec)

      val andBuilder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
      andBuilder.addArgs(constructBinaryExpr("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
        partitionColumn.name + "=" + partitionVal))
      andBuilder.addArgs(partTypeExpr)

      pathExprBuilder.addPreds(Predicate.newBuilder()
        .setExprNode(ExprNode.newBuilder().setExprBool(andBuilder)))

    }

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(pathExprBuilder.build())
        .setBaseOnly(true).setReturnType(1)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build)
    toCatalogTablePartitions(queryResponses, txn)
  }

  def listFilesByFilter(db: String, table: String, predicates: Seq[Expression],
                        txn : Option[TreeTxn]): Seq[FileStatus] = {
    val tableObj = getTable(db, table, txn)
    if (tableObj != null) {
      listFilesByFilter(tableObj, predicates, txn)
    }
    else {
      ArrayBuffer[FileStatus]()
    }
  }

  def listFilesWithStatsByFilter(db: String, table: String, predicates: Seq[Expression],
                        txn : Option[TreeTxn]): Seq[CatalogTableFile] = {
    val tableObj = getTable(db, table, txn)
    if (tableObj != null) {
      listFilesWithStatsByFilter(tableObj, predicates, txn)
    }
    else {
      ArrayBuffer[CatalogTableFile]()
    }
  }

  def listFilesByFilter(table: CatalogTable, predicates: Seq[Expression],
                        txn : Option[TreeTxn]): Seq[FileStatus] = {
    val pathExprBuilder = convertFilters(table, predicates)
    pathExprBuilder.addPreds(Predicate.newBuilder().
      setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY))

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(
      pathExprBuilder.build()).setBaseOnly(true).setReturnType(2)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    toFileStatuses(queryResponses, txn)
  }

  def listFilesWithStatsByFilter(table: CatalogTable, predicates: Seq[Expression],
                        txn : Option[TreeTxn]): Seq[CatalogTableFile] = {
    val pathExprBuilder = convertFilters(table, predicates)
    pathExprBuilder.addPreds(Predicate.newBuilder().
      setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY))

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(
      pathExprBuilder.build()).setBaseOnly(true).setReturnType(2)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    toCatalogTableFiles(queryResponses, txn)
  }

  def listFiles(db: String, table: String, txn : Option[TreeTxn]): Seq[FileStatus] = {
    val tableObj = getTable(db, table, txn)
    if (tableObj != null) {
      listFiles(tableObj, txn)
    }
    else {
      ArrayBuffer[FileStatus]()
    }
  }

  def listFilesWithStats(db: String, table: String, txn: Option[TreeTxn]): Seq[CatalogTableFile] = {
    val tableObj = getTable(db, table, txn)
    if (tableObj != null) {
      listFilesWithStats(tableObj, txn)
    }
    else {
      ArrayBuffer[CatalogTableFile]()
    }
  }

  def listFiles(table : CatalogTable, txn : Option[TreeTxn]) : Seq[FileStatus] = {
    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)
    val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)

    val partPred = constructBinaryPred("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")
    table.partitionColumnNames.foreach({ column_name =>
      pathExprBuilder.addPreds(partPred)
    })
    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(pathExprBuilder.
      addPreds(Predicate.newBuilder().setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build()).
        build()).setBaseOnly(true).setReturnType(2)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    toFileStatuses(queryResponses, txn)
  }

  def listFilesWithStats(table : CatalogTable, txn : Option[TreeTxn]) : Seq[CatalogTableFile] = {
    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)
    val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)

    val partPred = constructBinaryPred("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")
    table.partitionColumnNames.foreach({ column_name =>
      pathExprBuilder.addPreds(partPred)
    })
    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(pathExprBuilder.
      addPreds(Predicate.newBuilder().setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build()).
      build()).setBaseOnly(true).setReturnType(2)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build())
    toCatalogTableFiles(queryResponses, txn)
  }

  def listFiles(table : CatalogTable, partition : CatalogTablePartition,
                txn : Option[TreeTxn]) : Seq[FileStatus] = {
    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)
    val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)

    val partTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")
    val partTypePred = Predicate.newBuilder().setExprNode(partTypeExpr).build
    table.partitionSchema.foreach { partitionColumn =>
      val partitionValOption = partition.spec.get(partitionColumn.name)
      if (partitionValOption.isDefined) {
        val partitionVal = {
          if (partitionColumn.dataType.isInstanceOf[IntegralType]) {
            "%012d".format(partitionValOption.get.toLong)
          }
          else {
            partitionValOption.get
          }
        }
        val andBuilder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
        andBuilder.addArgs(constructBinaryExpr("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
          partitionColumn.name + "=" + partitionVal))
        andBuilder.addArgs(partTypeExpr)

        pathExprBuilder.addPreds(Predicate.newBuilder()
          .setExprNode(ExprNode.newBuilder().setExprBool(andBuilder)))

      }
      else {
        pathExprBuilder.addPreds(partTypePred)
      }
    }

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(pathExprBuilder.
        addPreds(Predicate.newBuilder().setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build()).
        build()).setBaseOnly(true).setReturnType(2)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build)
    toFileStatuses(queryResponses, txn)
  }

  def listFilesWithStats(table : CatalogTable, partition : CatalogTablePartition,
                txn : Option[TreeTxn]) : Seq[CatalogTableFile] = {
    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)
    val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)

    val partTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")
    val partTypePred = Predicate.newBuilder().setExprNode(partTypeExpr).build
    table.partitionSchema.foreach { partitionColumn =>
      val partitionValOption = partition.spec.get(partitionColumn.name)
      if (partitionValOption.isDefined) {
        val partitionVal = {
          if (partitionColumn.dataType.isInstanceOf[IntegralType]) {
            "%012d".format(partitionValOption.get.toLong)
          }
          else {
            partitionValOption.get
          }
        }
        val andBuilder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
        andBuilder.addArgs(constructBinaryExpr("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
          partitionColumn.name + "=" + partitionVal))
        andBuilder.addArgs(partTypeExpr)

        pathExprBuilder.addPreds(Predicate.newBuilder()
          .setExprNode(ExprNode.newBuilder().setExprBool(andBuilder)))

      }
      else {
        pathExprBuilder.addPreds(partTypePred)
      }
    }

    val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(pathExprBuilder.
      addPreds(Predicate.newBuilder().setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build()).
      build()).setBaseOnly(true).setReturnType(2)
    setTxnId(queryRequest, txn)

    val queryResponses = catalogStub.executeQuery(queryRequest.build)
    toCatalogTableFiles(queryResponses, txn)
  }

  def createPartition(table : CatalogTable, partition : CatalogTablePartition,
                      txn : Option[TreeTxn]) : Option[Boolean] = {
    val newTxn = {
      if (txn.isDefined) {
        txn
      }
      else {
        startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      }
    }

    val dbPred = constructDbPred(table.identifier.database.get)
    val tablePred = constructTablePred(table.identifier.table)

    val partTypeExpr = constructBinaryExpr("obj_type", ExprOpType.EXPR_OP_TYPE_EQUALS, "partition")

    var partExists = false
    val partitionColumnNames = table.partitionColumnNames.to[mutable.ArrayBuffer]
    val partitionSchema = table.partitionSchema
    // check if the partition exists at each level from bottom to top
    while (!partExists && !partitionColumnNames.isEmpty) {
      val pathExprBuilder = PathExpr.newBuilder().addPreds(dbPred).addPreds(tablePred)
      for (i <- 0 until partitionColumnNames.size) {
        val partitionColumn = partitionSchema(i)
        val partitionVal = TreeSerde.getPartitionVal(partitionColumn, partition.spec)
        val andBuilder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
        andBuilder.addArgs(constructBinaryExpr("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
          partitionColumn.name + "=" + partitionVal))
        andBuilder.addArgs(partTypeExpr)
        pathExprBuilder.addPreds(Predicate.newBuilder()
          .setExprNode(ExprNode.newBuilder().setExprBool(andBuilder)))
      }

      val queryRequest = ExecuteQueryRequest.newBuilder().setParseTree(pathExprBuilder)
          .setBaseOnly(true).setReturnType(1)
      setTxnId(queryRequest, newTxn)
      val queryResponses = catalogStub.executeQuery(queryRequest.build)
      partExists = queryResponses.hasNext
      if (partExists) {
        var lastResponse : Option[ExecuteQueryResponse] = None
        while (queryResponses.hasNext) {
          lastResponse = Some(queryResponses.next())
        }
        // if aborted, commit to deallocate resource on the server side
        if (lastResponse.get.hasAbort) {
          newTxn.get.setAbort(lastResponse.get.getAbort)
          commit(newTxn.get)
          return Some(false)
        }
      }
      else {
        partitionColumnNames.trimEnd(1)
      }
    }

    val partitionVals : mutable.Map[String, String] = mutable.Map.empty
    partitionColumnNames.foreach{ columnName =>
      partitionVals.put(columnName, partition.spec.get(columnName).getOrElse("DEFAULT"))
    }

    val commitRequest = newTxn.get.commitRequest.get
    val emptyStorage = CatalogStorageFormat(None, None, None, None, false,
      Map.empty[String, String])

    val emptyStats = TreeSerde.emptyStats(table)
    for ( i <- partitionColumnNames.length until table.partitionColumnNames.length - 1) {
      val columnName = table.partitionColumnNames(i)
      partitionVals.put(columnName, partition.spec.get(columnName).getOrElse("DEFAULT"))
      // create appropriate ancestor partition
      val parentPartition = CatalogTablePartition(partitionVals.toMap, emptyStorage,
          Map.empty, System.currentTimeMillis, -1, None)
      val objPath = "/" + table.identifier.database.get + "/" + table.identifier.table +
          TreeSerde.getPartId(table, parentPartition.spec)
      val partitionBson = TreeSerde.toBson(objPath, table, parentPartition)
      // add parent partition
      commitRequest.addWriteSet(Write.newBuilder()
        .setType(WriteType.WRITE_TYPE_ADD)
        .setIsLeaf(false)
        .setWriteValue(ByteString.copyFrom(partitionBson.getInternalBuffer, 0,
          partitionBson.getPosition))
        .setPathStr(objPath))

      // create empty base statistics
      val statsPath = objPath + "/stats"
      val statsBson = TreeSerde.toBson(statsPath, table, emptyStats)
      commitRequest.addWriteSet(Write.newBuilder()
        .setType(WriteType.WRITE_TYPE_ADD)
        .setIsLeaf(false)
        .setWriteValue(ByteString.copyFrom(statsBson.getInternalBuffer, 0,
          statsBson.getPosition))
        .setPathStr(statsPath))

    }

    // create leaf partition
    val objPath = "/" + table.identifier.database.get + "/" + table.identifier.table +
      TreeSerde.getPartId(table, partition.spec)
    val partitionBson = TreeSerde.toBson(objPath, table, partition)
    commitRequest.addWriteSet(Write.newBuilder()
      .setType(WriteType.WRITE_TYPE_ADD)
      .setIsLeaf(false)
      .setWriteValue(ByteString.copyFrom(partitionBson.getInternalBuffer, 0,
        partitionBson.getPosition))
      .setPathStr(objPath))

    // create empty base statistics
    val statsPath = objPath + "/stats"
    val statsBson = TreeSerde.toBson(statsPath, table, emptyStats)
    commitRequest.addWriteSet(Write.newBuilder()
      .setType(WriteType.WRITE_TYPE_ADD)
      .setIsLeaf(false)
      .setWriteValue(ByteString.copyFrom(statsBson.getInternalBuffer, 0,
        statsBson.getPosition))
      .setPathStr(statsPath))

    // if not part of the existing transaction, commit
    if (txn.isEmpty) {
      Some(commit(newTxn.get))
    }
    else {
      None
    }

  }

  // add given files to the table, returns true if successful
  def addFiles(table : CatalogTable, files : Seq[CatalogTableFile],
               txn : Option[TreeTxn] = None): Option[Boolean] = {
    val newTxn = {
      if (txn.isDefined) {
        txn
      }
      else {
        startTransaction(TxnMode.TXN_MODE_READ_WRITE)
      }
    }

    val commitRequest = newTxn.get.commitRequest.get
    val tablePartitionSchema = table.partitionSchema
    files.foreach{ file =>
      val objPath = "/" + table.identifier.database.get + "/" + table.identifier.table +
        TreeSerde.getFileId(table, file)
      val fileBson = TreeSerde.toBson(objPath, table, file)

      commitRequest.addWriteSet(Write.newBuilder()
        .setType(WriteType.WRITE_TYPE_ADD)
        .setIsLeaf(true)
        .setWriteValue(ByteString.copyFrom(fileBson.getInternalBuffer, 0,
          fileBson.getPosition))
        .setPathStr(objPath))

      // if file has statistics, merge delta to table and partition at every level
      if (file.stats.isDefined) {
        val mergeBson = TreeSerde.toMergeBson(table, file.stats.get)
        val mergeByteString = ByteString.copyFrom(mergeBson.getInternalBuffer, 0,
          mergeBson.getPosition)
        val partitionVals = scala.collection.mutable.Map.empty[String, String]
        val tablePath = "/" + table.identifier.database.get + "/" + table.identifier.table
        // update table statistics
        commitRequest.addWriteSet(Write.newBuilder()
          .setType(WriteType.WRITE_TYPE_MERGE)
          .setIsLeaf(false)
          .setWriteValue(mergeByteString)
          .setPathStr(tablePath + "/stats"))
        // update partition statistics
        tablePartitionSchema.foreach { partitionColumn =>
          partitionVals.put(partitionColumn.name, file.partitionValues.get(partitionColumn.name)
            .getOrElse("DEFAULT"))
          val statsPath = tablePath + TreeSerde.getPartId(table, partitionVals.toMap) + "/stats"

          commitRequest.addWriteSet(Write.newBuilder()
            .setType(WriteType.WRITE_TYPE_MERGE)
            .setIsLeaf(false)
            .setWriteValue(mergeByteString)
            .setPathStr(statsPath))
        }
      }
    }

    // if not part of the existing transaction, commit
    if (txn.isEmpty) {
      Some(commit(newTxn.get))
    }
    else {
      None
    }
  }

}

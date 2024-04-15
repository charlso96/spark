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

package org.apache.spark.tree.grpc

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JNull
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.bson.RawBsonDocument
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.tree.grpc.Catalog._
import org.json4s.DoubleJsonFormats.GenericFormat
import org.json4s.Formats.read

import java.net.URI
private[spark] class TreeExternalCatalog extends Logging {
  private implicit val formats = Serialization.formats(NoTypeHints) + new HiveURISerializer +
    new HiveDataTypeSerializer + new HiveMetadataSerializer + new HiveStructTypeSerializer
  private val channel_builder = ManagedChannelBuilder.forAddress("localhost", 9876).usePlaintext()
  private val channel = channel_builder.asInstanceOf[ManagedChannelBuilder[_]]build()

  lazy val catalog_stub = GRPCCatalogGrpc.newBlockingStub(channel)
  private class BufIterator(buf : Array[Byte]) {
    private var elem_size_ = ByteBuffer.wrap(buf.slice(0, Integer.BYTES)).getInt
    private var data_idx_ = Integer.BYTES
    private var next_ = Integer.BYTES + elem_size_
    private var valid_ = false

    if (next_ <= buf.length) {
      valid_ = true
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
        elem_size_ = ByteBuffer.wrap(buf.slice(next_, next_ + Integer.BYTES)).getInt
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
  def getDatabase(db: String): CatalogDatabase = {
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder().
      addPreds(Predicate.newBuilder().setOid(db).build()).build()).setBaseOnly(true).
      setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    if (query_responses.hasNext) {
      val query_response = query_responses.next()
      val response_buf = query_response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
        response_buf.size - buf_iter.dataIdx()).toJson(new JsonWriterSettings.Builder().
        outputMode(JsonMode.RELAXED).build())
        read[CatalogDatabase](json_str)
    }
    else {
      null
    }
  }

  def getTable(db: String, table: String): CatalogTable = {
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder().
      addPreds(Predicate.newBuilder().setOid(db).build()).addPreds(Predicate.newBuilder().
      setOid(table).build()).build()).setBaseOnly(true).setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    if (query_responses.hasNext) {
      val query_response = query_responses.next()
      val response_buf = query_response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
        response_buf.size - buf_iter.dataIdx()).toJson(new JsonWriterSettings.Builder().
        outputMode(JsonMode.RELAXED).build())
        read[CatalogTable](json_str)
    }
    else {
      null
    }
  }

  def listTables(db: String): Seq[String] = {
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder().
      addPreds(Predicate.newBuilder().setOid(db)).addPreds(Predicate.newBuilder().
      setWildcard(Catalog.Wildcard.WILDCARD_ANY).build())).setBaseOnly(true).setReturnType(1).
      build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val tables = ArrayBuffer[String]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      val table_bson = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
        response_buf.size - buf_iter.dataIdx())
      tables += table_bson.get("identifier").asInstanceOf[RawBsonDocument].get("table").asString().
        getValue()
    })
    tables
  }

  def listFiles(table : CatalogTable) : Seq[String] = {
    val path_expr_builder = PathExpr.newBuilder().addPreds(Predicate.newBuilder().setOid(table.
      identifier.database.get).build()).addPreds(Predicate.newBuilder().
      setOid(table.identifier.table).build())

    table.partitionColumnNames.foreach({ column_name =>
      path_expr_builder.addPreds(Predicate.newBuilder().
        setWildcard(Catalog.Wildcard.WILDCARD_ANY).build())
    })
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(path_expr_builder.
      addPreds(Predicate.newBuilder().setWildcard(Catalog.Wildcard.WILDCARD_ANY).build()).build()).
      setBaseOnly(true).setReturnType(2).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val files = ArrayBuffer[String]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      while (buf_iter.valid()) {
        val file_bson = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
          response_buf.size - buf_iter.dataIdx())
        files += file_bson.get("path").asString().getValue()
        buf_iter.next()
      }
    })
    files
  }

  def listFiles(table : CatalogTable, partition : CatalogTablePartition) : Seq[String] = {
    val path_expr_builder = PathExpr.newBuilder().addPreds(Predicate.newBuilder().setOid(table.
      identifier.database.get).build()).addPreds(Predicate.newBuilder().
      setOid(table.identifier.table).build())
    table.partitionColumnNames.foreach({ column_name =>
      if (partition.spec.contains(column_name)) {
        path_expr_builder.addPreds(Predicate.newBuilder().
          setOid(column_name + "=" + partition.spec.get(column_name)).build())
      }
      else {
        path_expr_builder.addPreds(Predicate.newBuilder().
          setWildcard(Catalog.Wildcard.WILDCARD_ANY).build())
      }
    })

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(path_expr_builder.
        addPreds(Predicate.newBuilder().setWildcard(Catalog.Wildcard.WILDCARD_ANY).build()).
        build()).setBaseOnly(true).setReturnType(2).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val files = ArrayBuffer[String]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      while (buf_iter.valid()) {
        val file_bson = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
          response_buf.size - buf_iter.dataIdx())
        files += file_bson.get("path").asString().getValue()
        buf_iter.next()
      }
    })
    files
  }

  // TODO : implement get partitions/files by filter
}

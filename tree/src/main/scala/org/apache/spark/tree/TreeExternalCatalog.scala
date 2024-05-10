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

import scala.collection.mutable.ArrayBuffer

import io.grpc.ManagedChannelBuilder
import org.bson.RawBsonDocument
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JNull
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.types._
import org.apache.spark.tree.grpc.Grpccatalog
import org.apache.spark.tree.grpc.Grpccatalog._
import org.apache.spark.tree.grpc.Grpccatalog.Predicate
import org.apache.spark.tree.grpc.GRPCCatalogGrpc
import org.apache.spark.unsafe.types.UTF8String


private[spark] class TreeExternalCatalog extends Logging {
  private implicit val formats = Serialization.formats(NoTypeHints) + new HiveURISerializer +
    new HiveDataTypeSerializer + new HiveMetadataSerializer + new HiveStructTypeSerializer
  private val channel = ManagedChannelBuilder.forAddress("localhost", 9876).usePlaintext().
    asInstanceOf[ManagedChannelBuilder[_]].build()
  val catalog_stub: GRPCCatalogGrpc.GRPCCatalogBlockingStub =
    GRPCCatalogGrpc.newBlockingStub(channel)
  val json_writer_setting : JsonWriterSettings = JsonWriterSettings.builder().
    outputMode(JsonMode.RELAXED).build()

  private class BufIterator(buf : Array[Byte]) {
    private var elem_size_ = ByteBuffer.wrap(buf.slice(0, Integer.BYTES)).
      order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
    private var data_idx_ = Integer.BYTES
    private var next_ = Integer.BYTES + elem_size_
    private var valid_ = false

    if (next_ <= buf.size) {
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
          Some(constructBinaryExpr(attr.name, expr_op_type, value))
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
            case LessThan(_, _) => ExprOpType.EXPR_OP_TYPE_GREATER_EQUALS
            case LessThanOrEqual(_, _) => ExprOpType.EXPR_OP_TYPE_GREATER
            case GreaterThan(_, _) => ExprOpType.EXPR_OP_TYPE_LESS_EQUALS
            case GreaterThanOrEqual(_, _) => ExprOpType.EXPR_OP_TYPE_LESS
          }
          Some(constructBinaryExpr(attr.name, expr_op_type, value))
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
    val path_expr_builder = PathExpr.newBuilder()
    path_expr_builder.addPreds(constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
      table.identifier.database.get))
    path_expr_builder.addPreds(constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
      table.identifier.table))

    table.partitionColumnNames.foreach{ col_name =>
      val and_builder = ExprBool.newBuilder().setOpType(ExprBoolType.EXPR_BOOL_TYPE_AND)
      filters.map(convert(col_name, _)).filter(!_.isEmpty)
        .foreach(expr => and_builder.addArgs(expr.get))
      val and = and_builder.build()
      if (and.getArgsCount == 0) {
        path_expr_builder.addPreds(Predicate.newBuilder().setWildcard(Wildcard.WILDCARD_ANY))
      }
      else {
        path_expr_builder.addPreds(Predicate.newBuilder()
            .setExprNode(ExprNode.newBuilder().setExprBool(and)))
      }
    }

    path_expr_builder
  }

  def getDatabase(db: String): CatalogDatabase = {
    val db_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
      db)
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(
        PathExpr.newBuilder().addPreds(db_pred))
        .setBaseOnly(true).setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    if (query_responses.hasNext) {
      val query_response = query_responses.next()
      val response_buf = query_response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
        response_buf.size - buf_iter.dataIdx()).toJson(json_writer_setting)
      // iterate over every query response
      while (query_responses.hasNext) {
        query_responses.next()
      }
      read[CatalogDatabase](json_str)
    }
    else {
      null
    }
  }

  def getTable(db: String, table: String): CatalogTable = {
    val db_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, db)
    val table_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, table)
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder().
        addPreds(db_pred).addPreds(table_pred).build()).setBaseOnly(true).setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    if (query_responses.hasNext) {
      val query_response = query_responses.next()
      val response_buf = query_response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
        response_buf.size - buf_iter.dataIdx()).toJson(json_writer_setting)
      while (query_responses.hasNext) {
        query_responses.next()
      }
      read[CatalogTable](json_str)
    }
    else {
      null
    }
  }

  def listTables(db: String): Seq[String] = {
    val db_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, db)
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(PathExpr.newBuilder()
        .addPreds(db_pred).addPreds(Predicate.newBuilder()
        .setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build())).setBaseOnly(true)
        .setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val tables = ArrayBuffer[String]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      while (buf_iter.valid()) {
        val table_bson = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
          response_buf.size - buf_iter.dataIdx())
        tables += table_bson.get("identifier").asInstanceOf[RawBsonDocument].get("table").
          asString().getValue()
        buf_iter.next()
      }
    })
    tables
  }

  def listPartitions(db: String, table: String): Seq[CatalogTablePartition] = {
    val table_obj = getTable(db, table)
    val db_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, db)
    val table_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, table)
    val path_expr_builder = PathExpr.newBuilder().addPreds(db_pred).addPreds(table_pred)
    // TODO take care of stats (only get partitions)
    table_obj.partitionColumnNames.foreach { _ =>
      path_expr_builder.addPreds(Predicate.newBuilder().
        setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build())
    }

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(
      path_expr_builder.build()).setBaseOnly(true).setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val partitions = ArrayBuffer[CatalogTablePartition]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      while (buf_iter.valid()) {
        val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
          response_buf.size - buf_iter.dataIdx()).toJson(json_writer_setting)
        partitions += read[CatalogTablePartition](json_str)
        buf_iter.next()
      }
    })
    partitions

  }

  def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression]):
      Seq[CatalogTablePartition] = {
    val table_obj = getTable(db, table)
    val path_expr_builder = convertFilters(table_obj, predicates)

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(
      path_expr_builder.build()).setBaseOnly(true).setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val partitions = ArrayBuffer[CatalogTablePartition]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      while (buf_iter.valid()) {
        val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
          response_buf.size - buf_iter.dataIdx()).toJson(json_writer_setting)
        partitions += read[CatalogTablePartition](json_str)
        buf_iter.next()
      }
    })
    partitions
  }

  def listPartitionsByFilter(table: CatalogTable, predicates: Seq[Expression]):
      Seq[CatalogTablePartition] = {
    val path_expr_builder = convertFilters(table, predicates)

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(
      path_expr_builder.build()).setBaseOnly(true).setReturnType(1).build()
    val query_responses = catalog_stub.executeQuery(query_request)
    val partitions = ArrayBuffer[CatalogTablePartition]()
    query_responses.forEachRemaining(response => {
      val response_buf = response.getObjList().toByteArray()
      val buf_iter = new BufIterator(response_buf)
      while (buf_iter.valid()) {
        val json_str = new RawBsonDocument(response_buf, buf_iter.dataIdx(),
          response_buf.size - buf_iter.dataIdx()).toJson(json_writer_setting)
        partitions += read[CatalogTablePartition](json_str)
        buf_iter.next()
      }
    })
    partitions
  }

  def listFilesByFilter(db: String, table: String, predicates: Seq[Expression]):
      Seq[String] = {
    val table_obj = getTable(db, table)
    val path_expr_builder = convertFilters(table_obj, predicates)
    path_expr_builder.addPreds(Predicate.newBuilder().
      setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY))

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(
      path_expr_builder.build()).setBaseOnly(true).setReturnType(2).build()
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

  def listFilesByFilter(table: CatalogTable, predicates: Seq[Expression]):
      Seq[String] = {
    val path_expr_builder = convertFilters(table, predicates)
    path_expr_builder.addPreds(Predicate.newBuilder().
      setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY))

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(
      path_expr_builder.build()).setBaseOnly(true).setReturnType(2).build()
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

  def listFiles(table : CatalogTable) : Seq[String] = {
    val db_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, table.
        identifier.database.get)
    val table_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
        table.identifier.table)
    val path_expr_builder = PathExpr.newBuilder().addPreds(db_pred).addPreds(table_pred)

    table.partitionColumnNames.foreach({ column_name =>
      path_expr_builder.addPreds(Predicate.newBuilder().
        setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build())
    })
    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(path_expr_builder.
      addPreds(Predicate.newBuilder().setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build()).
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

  def listFiles(table : CatalogTable, partition : CatalogTablePartition) : Seq[String] = {
    val db_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS, table.
      identifier.database.get)
    val table_pred = constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
      table.identifier.table)
    val path_expr_builder = PathExpr.newBuilder().addPreds(db_pred).addPreds(table_pred)
    table.partitionColumnNames.foreach({ column_name =>
      if (partition.spec.contains(column_name)) {
        path_expr_builder.addPreds(constructBinaryPred("obj_id", ExprOpType.EXPR_OP_TYPE_EQUALS,
          column_name + "=" + partition.spec.get(column_name)))
      }
      else {
        path_expr_builder.addPreds(Predicate.newBuilder().
          setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build())
      }
    })

    val query_request = ExecuteQueryRequest.newBuilder().setParseTree(path_expr_builder.
        addPreds(Predicate.newBuilder().setWildcard(Grpccatalog.Wildcard.WILDCARD_ANY).build()).
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

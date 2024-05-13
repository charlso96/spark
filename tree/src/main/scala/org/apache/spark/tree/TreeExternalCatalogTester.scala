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

import org.apache.spark.sql.execution.SparkSqlParser

private[spark] object TreeExternalCatalogTester {

  def main(args: Array[String]): Unit = {

    val sqlParser = new SparkSqlParser

    val db_name = args(0)
    val external_catalog = new TreeExternalCatalog()
    val db = external_catalog.getDatabase(db_name)
    printf(db.toString + "\n")
    val tables = external_catalog.listTables(db_name)
    printf(tables.toString() + "\n")
    val table = external_catalog.getTable(db_name, tables(0))
    val expr = sqlParser.parseExpression("part_field1 > 'part_field1=950'")
    print(expr.toJSON + "\n")
    val partitions = external_catalog.listPartitionsByFilter(db_name, tables(0), Seq(expr))
    partitions.foreach{ partition => printf(partition.toString + "\n")}
//    printf(table.toString() + "\n")
//    val partitions = external_catalog.listPartitions(db_name, tables(0))
//    partitions.foreach{ partition => printf(partition.toString + "\n")}
//    val files = external_catalog.listFiles(table)
//    printf(files.toString() + "\n")
  }

}

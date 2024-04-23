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
import java.time.Duration
import java.time.Instant

import org.apache.spark.sql.hive.HMSClientExt
import org.apache.spark.tree.TreeExternalCatalog


private[spark] class ExperimentUtil {
  val hms_ext = new HMSClientExt(Seq.empty)
  val hms = hms_ext.client
  val tree = new TreeExternalCatalog()
}

private[spark] object Experiment {
  def main(args: Array[String]): Unit = {
    if (args(0) == "exp1") {
      exp1(args(1), args(2))
    }
  }

  private def exp1(db_str : String, file_name : String) : Unit = {
    val exp_util = new ExperimentUtil
    val file_writer = new FileWriter(new File(file_name))

    file_writer.write("------ Experiment 1 ------\n")
    file_writer.write("------ Testing Tree ------\n")

    // initial call to establish connection
    var start_time = Instant.now()
    exp_util.tree.getDatabase(db_str)
    var end_time = Instant.now()

    start_time = Instant.now()
    val tree_db = exp_util.tree.getDatabase(db_str)
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for getDatabase()\n")

    start_time = Instant.now()
    val tree_tables = exp_util.tree.listTables(db_str)
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listTables()\n")

    start_time = Instant.now()
    val tree_table = exp_util.tree.getTable(db_str, tree_tables(0))
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for getTable()\n")

    start_time = Instant.now()
    val tree_partitions = exp_util.tree.listPartitions(db_str, tree_tables(0))
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listPartitions()\n")

    start_time = Instant.now()
    val tree_files = exp_util.tree.listFiles(tree_table)
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listFiles()\n")

    file_writer.write("------ Testing HMS ------\n")

    // initial call to establish connection
    start_time = Instant.now()
    exp_util.hms.getDatabase(db_str)
    end_time = Instant.now()

    start_time = Instant.now()
    val hms_db = exp_util.hms.getDatabase(db_str)
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for getDatabase()\n")

    start_time = Instant.now()
    val hms_tables = exp_util.hms.listTables(db_str)
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listTables()\n")

    start_time = Instant.now()
    val hms_table = exp_util.hms.getTable(db_str, hms_tables(0))
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for getTable()\n")

    start_time = Instant.now()
    val hms_partitions = exp_util.hms.listPartitions(db_str, hms_tables(0))
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listPartitions()\n")

    hms_partitions.foreach { partition =>
      exp_util.hms_ext.listFiles(partition)
    }
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listFiles()\n")

    file_writer.close()
  }
}


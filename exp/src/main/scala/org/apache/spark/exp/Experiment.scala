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
import java.util.Locale

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.hive.HMSClientExt
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.util.Utils



private[spark] class ExperimentUtil extends Logging {
  val hms_ext = new HMSClientExt(Seq.empty)
  val hms = hms_ext.client
  val tree = new TreeExternalCatalog()

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  private var isShellSession = false

  val env = sys.env
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

  val conf = toSparkConf()
  val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
  val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

  createSparkSession()

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

  def createSparkSession(): SparkSession = {
    try {
      val execUri = System.getenv("SPARK_EXECUTOR_URI")
      conf.setIfMissing("spark.app.name", "Spark shell")
      // SparkContext will detect this configuration and register it with the RpcEnv's
      // file server, setting spark.repl.class.uri to the actual URI for executors to
      // use. This is sort of ugly but since executors are started as part of SparkContext
      // initialization in certain cases, there's an initialization order issue that prevents
      // this from being set after SparkContext is instantiated.
      conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
      if (execUri != null) {
        conf.set("spark.executor.uri", execUri)
      }
      if (System.getenv("SPARK_HOME") != null) {
        conf.setSparkHome(System.getenv("SPARK_HOME"))
      }

      val builder = SparkSession.builder.config(conf)
      if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase(Locale.ROOT) == "hive") {
        if (SparkSession.hiveClassesArePresent) {
          // In the case that the property is not set at all, builder's config
          // does not have this value set to 'hive' yet. The original default
          // behavior is that when there are hive classes, we use hive catalog.
          sparkSession = builder.enableHiveSupport().getOrCreate()
          logInfo("Created Spark session with Hive support")
        } else {
          // Need to change it back to 'in-memory' if no hive classes are found
          // in the case that the property is set to hive in spark-defaults.conf
          builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
          sparkSession = builder.getOrCreate()
          logInfo("Created Spark session")
        }
      } else {
        // In the case that the property is set but not to 'hive', the internal
        // default is 'in-memory'. So the sparkSession will use in-memory catalog.
        sparkSession = builder.getOrCreate()
        logInfo("Created Spark session")
      }
      sparkContext = sparkSession.sparkContext
      sparkSession
    } catch {
      case e: ClassNotFoundException if isShellSession && e.getMessage.contains(
        "org.apache.spark.sql.connect.SparkConnectPlugin") =>
        logError("Failed to load spark connect plugin.")
        logError("You need to build Spark with -Pconnect.")
        sys.exit(1)
      case e: Exception if isShellSession =>
        logError("Failed to initialize Spark session.", e)
        sys.exit(1)
    }
  }


}

private[spark] object Experiment {
  def main(args: Array[String]): Unit = {
    if (args(0) == "exp1") {
      exp1(args(1), args(2))
    }
    if (args(0) == "exp2") {
      exp2(args(1), args(2))
    }
    if (args(0) == "deltaexp") {
      deltaexp(args(1), args(2))
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
    val tree_partitions = exp_util.tree.listPartitions(db_str, tree_tables(0), None)
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listPartitions()\n")

    start_time = Instant.now()
    val tree_files = exp_util.tree.listFiles(tree_table, None)
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

  private def exp2(db_str : String, file_name : String) : Unit = {
    val exp_util = new ExperimentUtil
    val file_writer = new FileWriter(new File(file_name))
    val sqlParser = new SparkSqlParser
    val tree_part_expr = sqlParser.parseExpression("part_field1 > 'part_field1=90000'")
    val hms_part_expr = sqlParser.parseExpression("part_field1 > 90000")

    file_writer.write("------ Experiment 2 ------\n")
    file_writer.write("------ Testing Tree ------\n")

    // initial call to establish connection
    var start_time = Instant.now()
    exp_util.tree.getDatabase(db_str)
    var end_time = Instant.now()

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
    val tree_partitions = exp_util.tree.listPartitionsByFilter(db_str, tree_tables(0),
        Seq(tree_part_expr), None)
    end_time = Instant.now()
    printf(tree_partitions.size.toString + "\n")
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listPartitionsByFilter()\n")

    start_time = Instant.now()
    val tree_files = exp_util.tree.listFilesByFilter(db_str, tree_tables(0),
      Seq(tree_part_expr), None)
    end_time = Instant.now()
    printf(tree_files.size.toString + "\n")
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listFilesByFilter()\n")

    file_writer.write("------ Testing HMS ------\n")

    // initial call to establish connection
    start_time = Instant.now()
    exp_util.hms.getDatabase(db_str)
    end_time = Instant.now()

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
    val hms_partitions = exp_util.hms.listPartitionsByFilter(db_str, hms_tables(0),
      Seq(hms_part_expr), "UTC")
    end_time = Instant.now()
    printf(hms_partitions.size.toString + "\n")
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listPartitionsByFilter()\n")

    hms_partitions.foreach { partition =>
      exp_util.hms_ext.listFiles(partition)
    }
    end_time = Instant.now()
    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listFilesByFilter()\n")

    file_writer.close()
  }

  private def deltaexp(db_str : String, file_name : String) : Unit = {
    val exp_util = new ExperimentUtil
    val file_writer = new FileWriter(new File(file_name))
    val delta_catalog = exp_util.sparkSession.sessionState.catalogManager
      .catalog("spark_catalog").asInstanceOf[DelegatingCatalogExtension]

    val table = delta_catalog.loadTable(Identifier.of(Array(db_str), "table1"))

    val base_relation = table.asInstanceOf[DeltaTableV2].toBaseRelation
      .asInstanceOf[HadoopFsRelation]
    val start_time = Instant.now()
    val partitions = base_relation.location.listFiles(Seq.empty, Seq.empty)
    val files = partitions.flatMap(partitions => partitions.files)
    val end_time = Instant.now()

    file_writer.write(Duration.between(start_time, end_time).toMillis().toString +
      " ms for listFiles()\n")


    file_writer.close()
    // files.foreach( file => printf(file.getPath.toString + "\n"))
  }
}





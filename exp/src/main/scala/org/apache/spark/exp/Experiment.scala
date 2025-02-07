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

import java.util.Locale

import scala.collection.mutable.HashMap

import org.apache.iceberg.spark.SparkCatalog

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension
import org.apache.spark.sql.hive.HMSClientExt
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.tree.TreeExternalCatalog
import org.apache.spark.util.Utils

private[spark] class DeltaUtil() extends Logging {
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

  val delta = sparkSession.sessionState.catalogManager
    .catalog("spark_catalog").asInstanceOf[DelegatingCatalogExtension]

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

private[spark] class IcebergUtil() extends Logging {
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

  val iceberg = sparkSession.sessionState.catalogManager
    .catalog("hive_prod").asInstanceOf[SparkCatalog]

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

private[spark] class TreeUtil(tree_address : String) extends Logging {
  val tree = new TreeExternalCatalog(tree_address)
}

private[spark] class HMSUtil() extends Logging {
  val hms_ext = new HMSClientExt(Seq.empty)
  val hms = hms_ext.client
}

private[spark] class ExperimentUtil(treeAddress: String = "localhost:9876") extends Logging {
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

  val hms_ext = new HMSClientExt(Seq.empty)
  val hms = hms_ext.client
  val tree = new TreeExternalCatalog(treeAddress)
  val delta = sparkSession.sessionState.catalogManager
    .catalog("spark_catalog").asInstanceOf[DelegatingCatalogExtension]
  val iceberg = sparkSession.sessionState.catalogManager
    .catalog("hive_prod").asInstanceOf[SparkCatalog]

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






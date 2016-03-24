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

package org.apache.spark.sql.execution.datasources.pf

import java.io.PrintStream
import java.net.{InetAddress, Socket}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.pf.PFRelation.{NEXT_CHUNK, executeCommand}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

import scala.io.BufferedSource

/**
  * Provides access to CSV data from pure SQL statements.
  */



class DefaultSource extends org.apache.spark.sql.execution.datasources.csv.DefaultSource with DataSourceRegister {


  override def shortName(): String = "pf"

  override def toString: String = "PF"

  override def equals(other: Any): Boolean = other.isInstanceOf[DefaultSource]


  //  val fileDesc : PFileDesc = ???


  //val paths = fileDesc.chunkPFArray.map(chunk  => "file:"+chunk.data_location)


  /**
    * Creates a new relation for data store in CSV given parameters and user supported schema.
    */
 /* override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {

    new PFRelation(
      None,
      paths,
      dataSchema,
      partitionColumns,
      parameters)(sqlContext)
  }*/
  override def buildInternalScan(sqlContext: SQLContext, dataSchema: StructType,
                                 requiredColumns: Array[String], filters: Array[Filter],
                                 bucketSet: Option[BitSet], inputFiles: Seq[FileStatus]
                                 , broadcastedConf: Broadcast[SerializableConfiguration],
                                 options: Map[String, String]): RDD[InternalRow] = {




    val pfFiles = inputFiles.filterNot(_.getPath.getName startsWith "_")


    val pathsString = inputFiles.map(_.getPath.toUri.toString)
    val header = dataSchema.fields.map(_.name)


    val  reply = PFRelation.executeCommand(NEXT_CHUNK+pfFiles.head)


    val  newFileStatus = reply.flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      SparkHadoopUtil.get.globPathIfNecessary(qualified)
    }.toArray
    val fileCatalog: FileCatalog =
      new HDFSFileCatalog(sqlContext, options, newFileStatus,None)

    super.buildInternalScan(sqlContext,dataSchema,requiredColumns,filters,bucketSet,fileCatalog.allFiles(),broadcastedConf,options)

  }


  /**
    * When possible, this method should return the schema of the given `files`.  When the format
    * does not support inference, or no valid files are given should return None.  In these cases
    * Spark will require that user specify the schema manually.
    */
  override def inferSchema(sqlContext: SQLContext, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {

    val path = files.filterNot(_.getPath.getName startsWith "_").map(_.getPath.toString).head

    val fileDesc = PFRelation.extractPFMetadata[PFileDesc](path)
    return fileDesc.structType

  }

  /**
    * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
    * be put here.  For example, user defined output committer can be configured here
    * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
    */
  override def prepareWrite(sqlContext: SQLContext, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = null
}

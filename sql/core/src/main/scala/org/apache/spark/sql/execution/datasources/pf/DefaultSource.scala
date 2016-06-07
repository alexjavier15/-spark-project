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

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

/**
  * Provides access to CSV data from pure SQL statements.
  */



class DefaultSource extends org.apache.spark.sql.execution.datasources.csv.DefaultSource with DataSourceRegister with Serializable {


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


    buildInternalScan(sqlContext,dataSchema,requiredColumns,filters,bucketSet,inputFiles,broadcastedConf,options)
  }

  def buildInternalScan(sqlContext: SQLContext, dataSchema: StructType,
                                 requiredColumns: Array[String], filters: Array[Filter],
                                 bucketSet: Option[BitSet], inputFiles: Seq[FileStatus]
                                 , broadcastedConf: Broadcast[SerializableConfiguration],
                                 options: Map[String, String], relation : HadoopPfRelation  = null): RDD[InternalRow] = {


    super.buildInternalScan(sqlContext,dataSchema,requiredColumns,filters,bucketSet,relation.location.allFiles(),broadcastedConf,options)

  }


  /**
    * When possible, this method should return the schema of the given `files`.  When the format
    * does not support inference, or no valid files are given should return None.  In these cases
    * Spark will require that user specify the schema manually.
    */
  override def inferSchema(sqlContext: SQLContext, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {

    val path = files.filterNot(_.getPath.getName startsWith "_").map(_.getPath.toString).head

     PFRelation.extractPFMetadata[PFileDesc](path).structType

  }

  /**
    * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
    * be put here.  For example, user defined output committer can be configured here
    * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
    */
  override def prepareWrite(sqlContext: SQLContext, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = null
}


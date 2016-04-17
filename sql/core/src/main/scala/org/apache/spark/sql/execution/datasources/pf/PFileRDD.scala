/*
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
import org.apache.hadoop.mapred.{FileSplit, InputSplit}
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.collection.immutable.Map
import scala.reflect.ClassTag

private[sql] class HadoopPartition(rddId: Int, idx: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

  /**
    * Get any environment variables that should be added to the users environment when running pipes
    * @return a Map with the environment variables and corresponding values, it could be empty
    */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = if (inputSplit.value.isInstanceOf[FileSplit]) {
      val is: FileSplit = inputSplit.value.asInstanceOf[FileSplit]
      // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
      // since its not removed yet
      Map("map_input_file" -> is.getPath().toString(),
        "mapreduce_map_input_file" -> is.getPath().toString())
    } else {
      Map()
    }
    envVars
  }
}

/**
  * Created by alex on 24.03.16.
  */
class PFileRDD[T: ClassTag](sc: SparkContext,
                            val rdds0: Seq[RDD[T]])  extends UnionRDD[T](sc, rdds0){







}
*/

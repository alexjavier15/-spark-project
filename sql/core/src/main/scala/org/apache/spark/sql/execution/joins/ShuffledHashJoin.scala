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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, JoinedRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}

/**
 * Performs an inner hash join of two child relations by first shuffling the data using the join
 * keys.
 */
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
    "numStreamedMatchedRows" -> SQLMetrics.createLongMetric(sparkContext, "number of streamed matched rows"),
     NUM_PARTITIONS_KEY ->SQLMetrics.createLongMetric(sparkContext, "number of partitions"),
    "numHashedMatchedRows" -> SQLMetrics.createLongMetric(sparkContext, "number of hashed matched rows")
    //NUM_ROWS_KEY-> SQLMetrics.createLongMetric(sparkContext, "number of rows")
  )

  override def outputPartitioning: Partitioning = joinType match {
    case Inner => PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftSemi => left.outputPartitioning
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x =>
      throw new IllegalArgumentException(s"ShuffledHashJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] = //UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil


  override def selectivity(): Double = {
    val numStreamedMatchedRows = longMetric("numStreamedMatchedRows")

    if(longMetric("numOutputRows").value.value <= 0)
      Double.MinValue
    else
      Seq( 1.0/right.getOutputRows , 1.0/numStreamedMatchedRows.value.value).foldLeft(getOutputRows.toDouble)(_*_)
  }
 // override def rows(): Long = children.map(_.rows()).product

  override def simpleHash: Int = {
    var h = 17
    h = h * 37 + left.simpleHash
    h = h * 37 + right.simpleHash
    h = h * 37 +  Some((leftKeys zip rightKeys).map { case (l, r) => EqualTo(l, r) }.
      reduceLeft(And)).map(_.semanticHash()).sum
    h
  }


  /*override def semanticHash: Int = {
    var h = 17
    h = h * 37 + left.semanticHash + right.semanticHash
    h = h * 37 +  Some((leftKeys zip rightKeys).map { case (l, r) => EqualTo(l, r) }.
        reduceLeft(And)).map(_.semanticHash()).sum

    h

  }*/

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numStreamedMatchedRows =longMetric("numStreamedMatchedRows")
   // val numRows = longMetric(NUM_ROWS_KEY)
    val hashTableSize = statistics match {

      case Some(s) if buildSide == BuildRight =>
                s.getOrElse("rightRows",64L).asInstanceOf[Int]
      case Some(s) if buildSide == BuildLeft =>
        s.getOrElse("leftRows",64L).asInstanceOf[Int]
       case _ => 64

    }

    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = HashedRelation(buildIter.map(_.copy()), buildSideKeyGenerator,hashTableSize)
      val joinedRow = new JoinedRow
      joinType match {
        case Inner =>

          hashJoin(streamIter, hashed, numOutputRows,Some(numStreamedMatchedRows))

        case LeftSemi =>
          hashSemiJoin(streamIter, hashed, numOutputRows)

        case LeftOuter =>
          val keyGenerator = streamSideKeyGenerator
          val resultProj = createResultProjection
          streamIter.flatMap(currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashed.get(rowKey), resultProj, numOutputRows)
          })

        case RightOuter =>
          val keyGenerator = streamSideKeyGenerator
          val resultProj = createResultProjection
          streamIter.flatMap(currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashed.get(rowKey), joinedRow, resultProj, numOutputRows)
          })

        case x =>
          throw new IllegalArgumentException(
            s"ShuffledHashJoin should not take $x as the JoinType")
      }
    }
  }
}

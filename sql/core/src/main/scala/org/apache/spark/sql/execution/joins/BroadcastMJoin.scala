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

import java.util.concurrent.ConcurrentMap

import com.google.common.collect.MapMaker
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.pf.PFRelation.CHUNK_NUM
import org.apache.spark.sql.execution.exchange.EnsureRequirements

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Performs an inner hash join of two child relations.  When the output RDD of this operator is
  * being constructed, a Spark job is asynchronously started to calculate the values for the
  * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
  * relation is not shuffled.
  */
case class BroadcastMJoin(
                           child: SparkPlan,
                           chunkedchild: Seq[SparkPlan],
                           baseRelations: Seq[SparkPlan],
                           subplans: Option[Seq[SparkPlan]]
                         )
  extends UnaryNode {

  private val chunkedSubplans: Seq[Seq[Int]] = makeBucktedSubplans
  private var currentSubplans: Seq[Seq[Int]] = Seq(Seq())
  var key = -1
  private val _hashConditions = mutable.Set[HashCondition]()
  private val _relationLengthToRelations = mutable.HashMap[Long, Set[CardinalityInfo]]()
  // Keeps track of all persisted RDDs
  private[this] val persistentHashTables = {
    val map: ConcurrentMap[Int, broadcast.Broadcast[HashedRelation]] = new MapMaker().
      weakValues().makeMap[Int, broadcast.Broadcast[HashedRelation]]()
    map.asScala
  }

  private[this] def initializeHashTables(): Unit = {
    baseRelations.
      map(baseRel => {
        val idx = baseRelations.indexOf(baseRel)
        val hashRelation = executeBroadcast[HashedRelation]
        persistHashTable(idx, hashRelation)

      })
  }


  private[this] def unifyHashConditions(): Unit = {
    // Start with the chunked plan ??
    addConditionsFromPlans(chunkedchild)

    subplans match {

      case Some(list) => addConditionsFromPlans(list)
      case _ => Unit

    }
  }

  private[this] def addConditionsFromPlans(plans: Seq[SparkPlan]): Unit =    plans foreach {
      _hashConditions ++= extractHashConditions(_) }


  private[this] def extractHashConditions(sparkPlan: SparkPlan): Set[HashCondition] = {

    sparkPlan match {

      case j: HashJoin => (extractHashConditions(j.left) ++
        extractHashConditions(j.right)) + HashCondition(j.leftKeys, j.rightKeys)
      case l: UnaryNode => extractHashConditions(l.child)
      case _ => Set.empty

    }


  }

  private[this] def persistHashTable(idx: Int, hashRelation: broadcast.Broadcast[HashedRelation]) {
    persistentHashTables(idx) = hashRelation

  }

  private[this] def makeBucktedSubplans(): Seq[Seq[Int]] = {

    val metadataList = baseRelations.map(relation => relation match {
      case Filter(_, d@DataSourceScan(_, rdd, _, metadata)) =>
        key = rdd.id
        metadata.get(CHUNK_NUM).getOrElse("1")
      case _ => "1"
    }
    )
    assert(!metadataList.contains(""))
    val numChunksPerRel = metadataList.map(_.toInt)

    def permuteWithoutOrdering(seq: Seq[Int]): Seq[Seq[Int]] = {
      seq match {

        case x :: Nil => Seq.iterate(Seq(1), seq.head)(x => Seq(x.head + 1))
        case _ =>
          for (y <- Seq.iterate(1, seq.head)(y => y + 1); x <- permuteWithoutOrdering(seq.tail))
            yield (y +: x)
      }
    }

    val res = permuteWithoutOrdering(numChunksPerRel)

    println(res)
    res
  }

  // def  streamed(start : Int ,iterator : Iterator[Int]) : Stream[(Int, Int)] = (start ,iterator.next())#::streamed(start+1,iterator)
  def getSubplansForChunk(relIdx: Int, chunkIdx: Int): Seq[Seq[Int]] = {
    assert(relIdx <= baseRelations.length)
    val qualified = chunkedSubplans.filter(subplan => subplan(relIdx).equals(chunkIdx))
    qualified
  }

  private def chunktoDrop(): (Int, Int) = {

    val allSubplans = currentSubplans
    //++newSubplans

    val mapped = allSubplans.flatMap(subplan => {
      val iterator = subplan.toIterator
      Seq.iterate((1, iterator.next), subplan.length)(x => (x._1 + 1, iterator.next))

    })

    val groupedChunks = mapped.groupBy(x => x)
    val numSuplansByChunk = groupedChunks.map(chunk => (chunk._1, chunk._2.length))

    numSuplansByChunk.minBy(_._2)._1

    // if there's other criteria jsut add here


  }

  private def chunkHasSubplans(relIdx: Int, chunkIdx: Int): Boolean = {

    getSubplansForChunk(relIdx, chunkIdx).nonEmpty

  }

  private def getNextChunk(relIdx: Int): Int = {


    val relation = baseRelations(relIdx)

    relation match {
      //    case DataSourceScan(_, _, fp :  pf.DefaultSource, metadata) =>  fp.getNextChunkID
      case _ => -1
    }


  }


  /**
    * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
    * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
    * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
    *
    * Note: the prepare method has already walked down the tree, so the implementation doesn't need
    * to call children's prepare methods.
    */
  override protected def doPrepare(): Unit = {


  }

  /*  private [this] def getNumOfBuckects(baseRelation : SparkPlan) : Int = {

      baseRelation match {

        case h : PFRelation =>

      }



    }*/


  private def updateSelectivities(sparkPlan: SparkPlan): Unit = {
    val joins = sparkPlan.extractNodes[BinaryNode]
    val baseRelations = sparkPlan.extractNodes[DataSourceScan]

    joins.foreach(join => {
      val baseRels: Set[SparkPlan] = sparkPlan.extractNodes[LeafNode].toSet
      val cardinality = join.getOutputRows
      val hj = join.asInstanceOf[HashJoin]
      val hashCondition = _hashConditions.find( condition => condition.equals(HashCondition(hj.leftKeys,hj.rightKeys))).get
      hashCondition.selectivity= join.selectivity()
      val cardInfo = new CardinalityInfo(baseRels, cardinality)
      if (_relationLengthToRelations.contains(join.numLeaves))
         _relationLengthToRelations(join.numLeaves) += cardInfo
      else
        _relationLengthToRelations += (join.numLeaves -> Set(cardInfo))
    })

    baseRelations.map(relation => new CardinalityInfo(Set(relation), relation.getOutputRows)).foreach(cardInfo => {
      if (_relationLengthToRelations.contains(1))
        _relationLengthToRelations(1) += cardInfo
      else
        _relationLengthToRelations += (1L -> Set(cardInfo))

    })


  }

  private def inferSelectivities(): Unit = {
    // Do selectivity inference iteratively

    def inferSelectivityFrom(relations: Set[SparkPlan], length: Long): Unit = {

      val below = _relationLengthToRelations(length - 1) // only left-deep plans!
      // val

    }

    val max = _relationLengthToRelations.keySet.max
    (3L to max).foreach(length => {


    })


  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (key != -1) {
      sqlContext.sparkContext.setLocalProperty("chunksFor" + key, "1")
      println("setting chunks for : " + key)
    }

    unifyHashConditions
    println("********Hash conditions*********")
    println(_hashConditions)
    sqlContext.setConf("spark.sql.mjoin", "false")
    sparkContext.union(chunkedchild.map(plan => {
      /*val  metrics = plan.executeBroadcastMetrics()
       println(metrics.value)

     val info = SparkPlanInfo.fromSparkPlan(child)
      println(info.metrics)
      SQLExecution.withExecution
      plan.execute()


      //val query   = sqlContext.executePlan(plan)
     // val executedPlan = query.executedPlan
      val executedPlan = plan
      println(plan.toString)

      executedPlan.executeCollect()
      executedPlan.printMetrics
      println ("Cost :" +executedPlan.planCost())
      updateSelectivities(executedPlan)
      println("cardinality map :")
      println(_relationLengthToRelations)*/
      val executedPlan = EnsureRequirements(this.sqlContext.conf)(plan)
      val rdd = executedPlan.execute
      rdd.cache
      rdd.collect
      println(plan.toString)
      executedPlan.printMetrics
      println("Cost :" + executedPlan.planCost)
      updateSelectivities(executedPlan)
      println("cardinality map :")
      println(_relationLengthToRelations)
      rdd
    }
    ))

    // this.chunkedchild.map(_.execute()).reduce(_.union(_));
  }

  override def output: Seq[Attribute] = child.output


}

class CardinalityInfo(plans: Set[SparkPlan], rows: Long) extends Serializable {
  override def toString: String = plans.map(plan => plan.nodeName).toString() +
    "\r\n" + " rows : " + rows

  override def hashCode(): Int = {
    plans.map(_.hashCode()).sum

  }


}

case class HashCondition(leftKeys: Seq[Expression],
                         rightKeys: Seq[Expression]) {
  var selectivity : Double = -1.0
  override def hashCode(): Int = {
    leftKeys.map(_.hashCode()).sum + rightKeys.map(_.hashCode()).sum

  }

  override def equals(o: scala.Any): Boolean =  o match {

      case HashCondition(l, r) if leftKeys.equals(l) && rightKeys.equals(r) => true
      case HashCondition(l, r) if leftKeys.equals(r) && rightKeys.equals(l) => true
      case _ => false

    }

  override def toString: String =  {

    leftKeys.zip(rightKeys).map( equal => "[ " +
      equal._1.toString +
      " = "+
      equal._2.toString+ " ]" ).mkString(",")

  }
}


















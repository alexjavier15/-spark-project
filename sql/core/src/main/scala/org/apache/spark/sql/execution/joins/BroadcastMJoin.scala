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
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.pf.HadoopPfRelation
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
                           baseRelations: Map[Int,Seq[SparkPlan]],
                           subplans: Option[Seq[SparkPlan]]
                         )
  extends UnaryNode {

  private var currentSubplans: Seq[Seq[Int]] = Seq(Seq())
  var key = -1
  private val numSampledRows : Int = 10000
  private var _bestPlan = child
  private var _samplingFactor : Option[Long] = None
  private var _pendingSubplans  =initSubplans()
  // Keeps track of all persisted RDDs
  private val _subplansMap = subplans.getOrElse(Nil).groupBy{
    subplan => subplan.simpleHash}.map{ case (a,b)=> a -> b.head }
  //private[this] val persistentHashTables = {
  //  val map: ConcurrentMap[Int, broadcast.Broadcast[HashedRelation]] = new MapMaker().
    //  weakValues().makeMap[Int, broadcast.Broadcast[HashedRelation]]()
    //7map.asScala
  //}



  private[this] def initSubplans():Iterator[Seq[SparkPlan]] ={
    /** For each element x in List xss, returns (x, xss - x) */


    def combine( chunks: List[Seq[SparkPlan]]): Seq[Seq[SparkPlan]] = {

      chunks match {
        case x :: Nil => x.map(Seq(_))
        case x :: xs :: Nil =>
          for (y <- x; ys <- xs)
            yield Seq(y,ys)
        case x::xs =>
          for (y <- x; ys <- combine(xs))
            yield y+:ys
      }
    }


    val res = combine(baseRelations.values.toList)



    res.toIterator

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


      subplans.getOrElse(Seq()).foreach( plan => {
        val selPlan = SelectivityPlan(plan)
       SelectivityPlan._selectivityPlanRoots+=(plan.simpleHash->selPlan)

      }

      )
    logInfo(_pendingSubplans.toString())
    logInfo("****Subplans****")
    logInfo(subplans.toString)
    logInfo("****Selectivity Plans****")
    logInfo(SelectivityPlan._selectivityPlanRoots.toString())
    logInfo("****SelectivityPlan nodes****")
    logInfo(SelectivityPlan._selectivityPlanNodes.toString())
    logInfo("****SelectivityPlan filters****")
    logInfo(SelectivityPlan._filterStats.toString())

  }


  private def updateStatisticsTo(sparkPlan : SparkPlan): Unit ={

        sparkPlan match {

          case hj :HashJoin =>
            val stats = SelectivityPlan._selectivityPlanNodes.
              get(hj.simpleHash).foreach{
              case a @ HashCondition(l,r,c,s) =>
                val leftRows  = l.rows/l.numPartitionsFromExecution.getOrElse(2L)
                val rightRows  = r.rows/r.numPartitionsFromExecution.getOrElse(2L)
                hj.statistics=Some(Map( "leftRows"-> leftRows,"rightRows"->rightRows))

            }
            updateStatisticsTo(hj.left)
          case u : UnaryNode => updateStatisticsTo(u.child)
          case _ =>


        }

  }
  private def updateSelectivities(sparkPlan: SparkPlan): Unit = {

    SelectivityPlan.updatefromExecution(sparkPlan)

    logInfo("****Selectivity Plans****")
    logInfo(SelectivityPlan._selectivityPlanRoots.toString())
    val bestPlan =SelectivityPlan._selectivityPlanRoots.minBy{ case (hc,selPlan) => selPlan.planCost()  }
    logInfo("****Min  plan****")
    logInfo(bestPlan.toString())
    val oldPlan = _bestPlan
    _bestPlan=_subplansMap.getOrElse(bestPlan._1,oldPlan)
    logInfo("****New Optimized plan****")
    logInfo(_bestPlan.toString())

  }

  private[this] def getPartition0RDD(plan: SparkPlan): SparkPlan = {

    val seed : Long = System.currentTimeMillis()

    plan match {
      case scan@DataSourceScan(o, rdd, rel, metadata) =>
        val withReplace = false

        DataSourceScan(o, rdd.sample(withReplace,0.1,seed), rel, metadata)
      case _ => plan
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {

    val rddBuffer = mutable.ArrayBuffer[RDD[InternalRow]]()
    sqlContext.setConf("spark.sql.mjoin", "false")
    var executedRDD: RDD[InternalRow] = null
    var duration: Long = 0L
    var tested = false
    while (_pendingSubplans.hasNext) {

      val subplan = _pendingSubplans.next().map { plan => plan.simpleHash -> plan }.toMap
      val start = System.currentTimeMillis()
      if (!tested) {
        logInfo("EXECUTING:")

        logInfo(subplan.toString())
        logInfo("BEFORE TRANSFORMATION:")
        logInfo(_bestPlan.toString)

        val newPlan = _bestPlan transform {

          case scan@DataSourceScan(_,_,h :  HadoopPfRelation,_) =>
            val ds = subplan.get(h.uniqueID).get
            getPartition0RDD(ds)


        }


        logInfo("AFTER TRANSFORMATION:")

        logInfo(newPlan.toString)

        val executedPlan = EnsureRequirements(this.sqlContext.conf)(newPlan)
        val rdd = executedPlan.execute()

        if (sqlContext.conf.mJoinSamplingEnabled) {

          rdd.count()

          executedPlan.printMetrics
          tested = true
          logInfo("Cost :" + executedPlan.planCost)
          updateSelectivities(executedPlan)
          updateStatisticsTo(_bestPlan)
          rdd.unpersist(false)
         return  EnsureRequirements(this.sqlContext.conf)(_bestPlan).execute()

        } else {
         // rdd.persist(MEMORY_AND_DISK)
          rdd.count()
          executedPlan.printMetrics
          logInfo("Cost :" + executedPlan.planCost)
          updateSelectivities(executedPlan)
          rddBuffer += rdd
        }




      }
      val end = System.currentTimeMillis()
      duration += (end - start)

    }
    logInfo("Duration Total: "+ duration)


    sparkContext.union(rddBuffer)


  }

  override def output: Seq[Attribute] = child.output


}

case class FilterStatInfo( filter : Expression
                           ) extends Serializable {

  private[this] var _selectivity : Double  = 0
  private[this] val  _derived : mutable.Set[FilterStatInfo] = mutable.Set[FilterStatInfo]()

  def selectivity : Double = _selectivity

  override def toString: String = filter.toString + " , [ Sel :"+ selectivity + " ]"

  def setSelectivity(selectivity : Double): Unit ={

    selectivity match {
      case s if s < 0.0 => 0.0
      case s if s > 1.0 => 1.0
      case s => this._selectivity = s

    }

  }

  override def equals(that: scala.Any): Boolean = that match {
    case FilterStatInfo(f) => f.semanticEquals(filter)
    case _ => false
  }

  override def hashCode(): Int = filter.semanticHash()
}
object SelectivityPlan {

  val _selectivityPlanNodes = mutable.HashMap[Int, SelectivityPlan]()
  val _selectivityPlanNodesSemantic = mutable.HashMap[Int, mutable.Set[SelectivityPlan]]()
  val _selectivityPlanRoots = mutable.HashMap[Int, SelectivityPlan]()

  val _filterStats = mutable.HashMap[Int, FilterStatInfo]()

  def apply(sparkPlan: SparkPlan): SelectivityPlan = fromSparkPlan(sparkPlan)

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }


  private[this] def joinSemanticHash(leftHashcode: Int, rightHashCode: Int): Int = {

    var h = 17
    h = h * 37 + leftHashcode + rightHashCode
    h
  }

  private[this] def getQualifiedKeys(sparkPlan :SparkPlan, keys : Seq[Expression]): Seq[Seq[Expression]] ={
    sparkPlan match {

        //TODO not verification of validity for equalities
      case join: HashJoin =>

        val rightKeys = join.rightKeys
        val leftQualified = getQualifiedKeys(join.left, join.leftKeys)
        getQualifiedKeys(join.right, join.rightKeys)
        updateFilterStats(leftQualified, rightKeys, sparkPlan.selectivity())
        _selectivityPlanNodes.get(join.simpleHash).get.setRowsFromExecution(sparkPlan.getOutputRows)
        _selectivityPlanNodesSemantic.getOrElse(join.semanticHash,Set.empty).foreach(_.setRowsFromExecution(join.getOutputRows))

        leftQualified ++ Seq(join.rightKeys)

      case u: UnaryNode => u match {
        case p@Project(_, child) => fromSparkPlan(child)
        case f@Filter(condition, child@DataSourceScan(outputset, _, _, _)) =>
          val semanticHashCode = f.simpleHash
          val node = _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
            ScanCondition(outputset, Some(condition), f.simpleHash)
          })
          node.setRows(f.getOutputRows)
          getQualifiedKeys(u.child, keys)
        case f@Filter(condition,
        child@HolderDataSourceScan(child0@DataSourceScan(outputset, _, _, _))) =>
          //TODO
          val semanticHashCode = f.simpleHash
          val node = _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
            ScanCondition(outputset, Some(condition), f.simpleHash)
          })
          node.setRows(f.getOutputRows)
          getQualifiedKeys(u.child, keys)
        case _ => getQualifiedKeys(u.child, keys)
      }

        getQualifiedKeys(u.child, keys)
      case u: LeafNode =>

        Seq(keys)
    }
  }
  private def updateFilterStats( qualifiedKeys : Seq[Seq[Expression]],
                                 targetKeys: Seq[Expression],
                                 sel : Double): Unit = {
    val filterStatInfos = getOrCreateFilters(qualifiedKeys,targetKeys)
    filterStatInfos foreach { fInfo =>
      fInfo.setSelectivity(sel)
    }

  }

  private[this] def getOrCreateFilters( leftChildKeys : Seq[Seq[Expression]],
                                        rightKeys : Seq[Expression]) : Seq[FilterStatInfo] ={

    leftChildKeys map { keys =>
      val condition = (keys zip rightKeys).map { case (l, r) => EqualTo(l, r) }.reduceLeft(And)
      _filterStats.getOrElseUpdate(condition.semanticHash(),FilterStatInfo(condition))
    }

  }
  def updatefromExecution(sparkPlan : SparkPlan) : Unit ={

    sparkPlan match {
      case join: HashJoin =>
        val rightKeys = join.rightKeys
        val leftQualifiedKeys = getQualifiedKeys(join.left,join.leftKeys)
        getQualifiedKeys(join.right, join.rightKeys)

        updateFilterStats(leftQualifiedKeys, rightKeys,sparkPlan.selectivity())
        //TODO unsafe  operation
        _selectivityPlanNodesSemantic.getOrElse(join.semanticHash,Set.empty).foreach {
          selPlan =>
            selPlan.setRowsFromExecution(join.getOutputRows)
            selPlan.setNumPartitionsFromExecution(sparkPlan.getNumPartitions)
        }
      case u: UnaryNode => updatefromExecution(u.child)

      case u: LeafNode =>

    }

  }

  private[this] def fromSparkPlan(sparkPlan: SparkPlan): SelectivityPlan = {


   sparkPlan match {
      case join: HashJoin =>
        val simpleHashCode = join.simpleHash
        val semanticHashCode = join.semanticHash

        val otherConditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)
        val condition = ((join.leftKeys zip join.rightKeys).map { case (l, r) => EqualTo(l, r) }
          ++ otherConditions).reduceLeft(And)
        val filterStatInfo = _filterStats.getOrElseUpdate(condition.semanticHash(),FilterStatInfo(condition) )
        _selectivityPlanNodes.getOrElseUpdate(simpleHashCode, {

        val newNode = HashCondition(fromSparkPlan(join.left),
            fromSparkPlan(join.right),
            filterStatInfo, simpleHashCode)
         _selectivityPlanNodesSemantic.getOrElseUpdate(semanticHashCode, mutable.Set[SelectivityPlan]())+=newNode

          newNode
        })

      case u: UnaryNode => u match {
        case p@Project(_, child) => fromSparkPlan(child)
        case f@Filter(condition, child@DataSourceScan(outputset, _, _, _)) =>
          val semanticHashCode = f.simpleHash
          _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
            ScanCondition(outputset, Some(condition), f.simpleHash)
          })
        case f@Filter(condition,
        child@HolderDataSourceScan(child0@DataSourceScan(outputset, _, _, _))) =>
          //TO-DO
          val semanticHashCode = f.simpleHash
          _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
            ScanCondition(outputset, Some(condition), f.simpleHash)
          })

        case _ => fromSparkPlan(u.child)
      }

      case _ => null


    }
  }

}

trait SemanticHashHelper{

  self: SelectivityPlan =>

  val semanticHash : Int

}
abstract class SelectivityPlan()
  extends TreeNode[SelectivityPlan]  with SemanticHashHelper {
  private[sql] val FETCH_TUPLE_COST : Double = 1.0
  private[sql] val CPU_TUPLE_COST : Double = 0.001
  private[sql] val HASH_QUAL_COST : Double = 0.00025
  private[sql] val CPU_OPERATOR_COST : Double = 0.00025

  private[sql] var rowsFromExecution : Option[Long] = None
  private[sql] var numPartitionsFromExecution : Option[Long] = None
  private var _rows : Long = 1
  // for future use in average
  private[sql] var numSelUpdates : Int = 0
  private[sql] var numrowsUpdates : Int = 0
  val shortName : String

  def selectivity : Double
  def rows : Long =  rowsFromExecution.getOrElse(_rows)


  def setRowsFromExecution(rows : Long): Unit ={
    rowsFromExecution= Some(rows)
    setRows(rows)
  }
  def setNumPartitionsFromExecution(num : Long): Unit ={
    numPartitionsFromExecution= Some(num)

  }
  def setRows(rows : Long): Unit ={

    rows match {
      case r if r <= 0 => 1
      case r => this._rows = r

    }
  }
  def planCost() : Double = {
    throw new UnsupportedOperationException(s"$nodeName does not implement plan costing")
  }

  override def hashCode(): Int = {

    throw new UnsupportedOperationException(s"$shortName does not implement hashCode")

  }
  override def equals(o: scala.Any): Boolean =  {

    throw new UnsupportedOperationException(s"$shortName does not implement equals")
  }

  /** String representation of this node without any children */
  override def simpleString: String = s"$nodeName $argString $rows $planCost() $selectivity".trim
}

case class ScanCondition( outputSet : Seq[Attribute],
                          filter : Option[Expression] = None,
                          override val semanticHash: Int) extends SelectivityPlan{

  override val shortName: String = "scanCondition"


  override def selectivity: Double = 1.0

  override def hashCode(): Int = {


    var h = 17
    outputSet.foreach( o => {
      h = h * 37 + o.semanticHash()

    })
    filter.foreach( o => {
      h = h * 37 + o.semanticHash()

    })
    h
  }

  /**
    * Returns a Seq of the children of this node.
    * Children should not change. Immutability required for containsChild optimization
    */
  override def children: Seq[ScanCondition] = Nil

  override def planCost(): Double = rows

  /*override def toString: String =  {

     super.toString+  s"${outputSet.toString}${filter.getOrElse("").toString}"
    }*/
  override def equals(that: Any): Boolean = that match {

    case ScanCondition(o, f,_) => (o zip outputSet).map{case (a,b)=> a.semanticEquals(b)}.reduceRight(_&&_) &&
      (filter zip f).map{case (a,b)=> a.semanticEquals(b)}.reduceRight(_&&_)
    case _ => false

  }
}

case class HashCondition(left : SelectivityPlan,
                         right : SelectivityPlan,
                         condition : FilterStatInfo,
                         override val  semanticHash : Int) extends SelectivityPlan{

  override val shortName: String = "hashCondition"
  private var optimizedSelectivity: Option[Double] = None



  override def hashCode(): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation


    var h = 17
    h = h * 37 + condition.hashCode()
    h = h * 37 + left.hashCode()
    h = h * 37 + right.hashCode()

    h
  }


  override def selectivity: Double = {
    if(rowsFromExecution.isDefined){
      val sel = rowsFromExecution.get.asInstanceOf[Double]/ children.map(_.rows).product
      sel
    }
    else
    condition.selectivity
  }

  override def rows: Long =  rowsFromExecution.getOrElse(computeRows())

  private[this] def computeRows(): Long = {

    val computedRows = children.map(_.rows).product
    (computedRows * selectivity).toLong

  }

  override def equals(o: scala.Any): Boolean =  o match {

      case HashCondition(l, r, c,_) => c.equals(condition)  && l.equals(left) && r.equals(right)

      case _ => false

    }
  override def planCost(): Double = {

    var startup_cost = right.planCost + left.planCost
    startup_cost += (CPU_OPERATOR_COST  + CPU_TUPLE_COST) * right.rows
    var run_cost = HASH_QUAL_COST * left.rows * right.rows * 2 * 0.75
    run_cost += CPU_OPERATOR_COST * left.rows
    run_cost += CPU_TUPLE_COST * rows
    startup_cost + run_cost

  }
  /*override def toString: String =  {

    super.toString + s"$shortName${condition.toString}${left.toString}${right.toString}"
  }*/




  override def children: Seq[SelectivityPlan] = Seq(left) ++ Seq(right)
}
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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.pf.HadoopPfRelation
import org.apache.spark.sql.execution.exchange.EnsureRequirements

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

/**
  * Performs an inner hash join of two child relations.  When the output RDD of this operator is
  * being constructed, a Spark job is asynchronously started to calculate the values for the
  * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
  * relation is not shuffled.
  */
case class BroadcastMJoin(
                           child: SparkPlan,
                           @transient baseRelations: Map[Int,Seq[SparkPlan]],
                           @transient subplans: Option[Seq[logical.LogicalPlan]]
                         )
  extends UnaryNode {

  private var currentSubplans: Seq[Seq[Int]] = Seq(Seq())
  var key = -1
  var found = false
  private val numSampledRows : Int = 10000
  private var _bestPlan = child
  private var _samplingFactor : Option[Long] = None
  private var _pendingSubplans  =initSubplans()
  private var _lastCost = 0.0
  private val samplingFactor = 0.1
  private val bucketSize =  8.0 * 1024*1024
  private val split = Array.fill(10)(samplingFactor)
  // Keeps track of all persisted RDDs
  private val _subplansMap = subplans.getOrElse(Nil).groupBy{
    subplan => subplan.simpleHash}.map{ case (a,b)=> a -> b.head }
  //private[this] val persistentHashTables = {
  //  val map: ConcurrentMap[Int, broadcast.Broadcast[HashedRelation]] = new MapMaker().
    //  weakValues().makeMap[Int, broadcast.Broadcast[HashedRelation]]()
    //7map.asScala
  //}


  private def combine[A]( chunks: List[Seq[A]]): Seq[Seq[A]] = {

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

  private[this] def initSubplans():Iterator[Seq[SparkPlan]] ={
/** For each element x in List xss, returns (x, xss - x)   **/
    val res = combine[SparkPlan](baseRelations.values.toList)
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

    //logInfo(child.toString)
    //System.exit(0)

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
  private def updateSelectivities(): Unit = {



    logInfo("****Selectivity Plans****")
    logInfo(SelectivityPlan._selectivityPlanRoots.toString())
    val validPlans = SelectivityPlan._selectivityPlanRoots.filter{
      case (hc,selPlan)  =>selPlan.isValid
      case _ => false

    }
    val bestPlan =validPlans.
      minBy{
        case (hc,selPlan) => selPlan.planCost()
        case _ => Double.MaxValue
      }
    logInfo("****Min  plan****")
    logInfo(bestPlan.toString())
    val oldPlan = _bestPlan
    val _bestLogical = _subplansMap(bestPlan._1)
    found =  bestPlan._1 != _bestPlan.semanticHash
      val queryExecution = new QueryExecution(sqlContext, _bestLogical)



      _bestPlan = queryExecution.sqlContext.sessionState.planner.
        plan(queryExecution.sqlContext.sessionState.optimizer.execute(_bestLogical)).next
      logInfo("****New Optimized plan****")
      logInfo(_bestPlan.toString())
      logInfo("****SelectivityPlan filters****")
      logInfo(SelectivityPlan._filterStats.toString())


  }

  private[this] def getPartition0RDD(plan: SparkPlan): SparkPlan = {

    val seed : Long = System.currentTimeMillis()

    plan match {
      case f @ Filter(_,child) => plan
      /*case scan@DataSourceScan(o, rdd, rel, metadata) =>
        val withReplace = false
        DataSourceScan(o, rdd.sample(withReplace,0.2,seed), rel, metadata)*/
      case _ => plan
    }
  }


  def findRootJoin(plan: SparkPlan): SparkPlan = {

    plan match {

      case j  : HashJoin => j
      case node: UnaryNode => findRootJoin(node.child)
      case _ => throw new IllegalArgumentException("Only UnaryNode must be above a Join")
    }

  }

  def optimizeJoinOrderFromJoinExecution(): Unit ={


    /*val subplan = _pendingSubplans.next().map { plan => plan.semanticHash -> plan }.toMap
    val start = System.currentTimeMillis()
    val rddIds = Array.fill(baseRelations.size)(0 until 10).toList
    val rddMap = HashMap[Int, Array[RDD[InternalRow]]]()
    val relationIndex = baseRelations.keySet.toSeq

    val rddPermutations0 = combine[Int](rddIds)
    val rddPermutations = rddPermutations0.toIterator

    val  trainingPlans = training.map {
      plan => {
        plan transformUp {

          case join : HashJoin =>
            LocalLimit(100000,join)

          case scan@DataSourceScan(_, rdd0, h: HadoopPfRelation, _) =>
            val ds = subplan.get(h.semanticHash).get
            assert(ds.semanticHash == scan.semanticHash)
            rddMap += ds.semanticHash -> ds.execute().randomSplit(split)
            ds

        }


      }

    }

      logInfo("EXECUTING:")

      logInfo(subplan.toString())
      logInfo("BEFORE TRANSFORMATION:")
      logInfo(_bestPlan.toString)


      while (rddPermutations.hasNext && !found) {
        val partitionSeq = (relationIndex zip rddPermutations.next()).toMap

        trainingPlans.foreach {
          join => {

            val newPlan = join transformUp {
              /*  case join @ShuffledHashJoin(
                leftKeys, rightKeys, Inner, buildSide, condition, left, right)=>
                  BroadcastHashJoin(
                    leftKeys, rightKeys, Inner, BuildRight, condition, left, right)*/

              case scan@DataSourceScan(output, rdd0, h: HadoopPfRelation, metadata) =>
                val ds = subplan.get(h.semanticHash).get
                assert(ds.semanticHash == scan.semanticHash)
                val newRDD = rddMap(h.semanticHash)(partitionSeq(h.semanticHash))
                DataSourceScan(output, newRDD, h, metadata)

            }


            logInfo("AFTER TRANSFORMATION:")

            logInfo(newPlan.toString)
            newPlan.resetChildrenMetrics
            newPlan.printMetrics
            val executedPlan = EnsureRequirements(this.sqlContext.conf)(newPlan)
            val rdd = executedPlan.execute()
            rdd.count


            executedPlan.printMetrics

            logInfo("Cost :" + executedPlan.planCost)
            SelectivityPlan.updatefromExecution(executedPlan)
            //updateStatisticsTo(_bestPlan)
            rdd.unpersist(false)
          }
        }



        updateSelectivities
        rddMap.values.foreach{
          array => {
            array.foreach(
              _.unpersist(false)

            )
          }
        }

      }

*/
  }

  def optimizeJoinOrderFromELS(): Unit ={
    sparkContext.withScope {

      val seed = System.currentTimeMillis()
      val currSubplan = _pendingSubplans.next()
      val subplan = currSubplan.map { plan => plan.semanticHash -> plan }.toMap

      val columnStatPlans = JoinOptimizer.joinOptimizer.columnStatPlans.map {
        case (attribute ,plan) => {
          val physicalPlan = sqlContext.sessionState.planner.plan(plan).next()
          println(physicalPlan)
          val transformedPlan =physicalPlan  transformUp {
            case scan@DataSourceScan(output, rdd0, h: HadoopPfRelation, metadata)=>
              val ds  = subplan(h.semanticHash)
              assert(ds.semanticHash == scan.semanticHash)
                LocalLimit(10000,ds)
          }
          attribute -> transformedPlan
        }
      }

      val localPredicateStatPlans = JoinOptimizer.joinOptimizer.localPredicatesStats.map {
        case (filter ,plan) => {
          val physicalPlan = sqlContext.sessionState.planner.plan(plan).next()
          println(physicalPlan)
          val transformedPlan =physicalPlan  transformUp {
            case scan@DataSourceScan(output, rdd0, h: HadoopPfRelation, metadata)=>
              val ds  = subplan(h.semanticHash)
              assert(ds.semanticHash == scan.semanticHash)
              ds
          }
          filter -> transformedPlan
        }
      }

      val localPredicateMap = localPredicateStatPlans.map {
        case (condition ,plan) => {
          plan.resetChildrenMetrics
          val  count = EnsureRequirements(this.sqlContext.conf)(plan).execute().count()
          condition -> count
        }
      }

      val distinctsAttrMap = columnStatPlans.map {
        case (attribute ,plan) => {
          plan.resetChildrenMetrics
        val  count = EnsureRequirements(this.sqlContext.conf)(plan).execute().count()
          attribute -> count
        }
      }

      logInfo("****************LOCAL PREDICATES***************")
      localPredicateStatPlans.foreach(t => t._2.printMetrics)
      logInfo("*******************************")
      println(localPredicateMap)
      columnStatPlans.foreach(t => t._2.printMetrics)
      logInfo("****************COLUMN STATS***************")
      println(distinctsAttrMap)

      found = true
      // System.exit(0)

    }


  }

  override protected def doExecute(): RDD[InternalRow] = {


    val rddBuffer = ArrayBuffer[RDD[InternalRow]]()
    sqlContext.setConf("spark.sql.mjoin", "false")
    var executedRDD: RDD[InternalRow] = null
    var duration: Long = 0L
    var tested = false
    var passes = 0
    while (_pendingSubplans.hasNext && !found) {
      //optimizeJoinOrderFromJoinExecution
      optimizeJoinOrderFromELS()
    }

    EnsureRequirements(this.sqlContext.conf)(findRootJoin(_bestPlan)).execute()
  }


  override def output: Seq[Attribute] = child.output


}

case class FilterStatInfo( filter : Expression
                           , children : Seq[FilterStatInfo] = Seq() ) extends Serializable {

  private[this] var _selectivity : Double  = Double.MinValue
  private[this] val  _derived : HashSet[FilterStatInfo] = HashSet[FilterStatInfo]()

  def selectivity : Double = {
    if (_selectivity < 0 && children.size > 1)
      children.map(_.selectivity).product
    else _selectivity
  }

  override def toString: String = filter.toString + " , [ Sel :"+ selectivity + " ]"

  def setSelectivity(selectivity : Double): Unit ={

    selectivity match {
      case s if s < 0.0 => 0.0
      case s if s > 1.0 => 1.0
      case s if _selectivity < 0.0 => this._selectivity = s

    }

  }


  override def equals(that: scala.Any): Boolean = that match {
    case f : FilterStatInfo => f.filter.semanticEquals(filter)
    case _ => false
  }

  override def hashCode(): Int = filter.semanticHash()
}

object SelectivityPlan {

  val _selectivityPlanNodes = HashMap[Int, SelectivityPlan]()
  val _selectivityPlanNodesSemantic = HashMap[Int, HashSet[SelectivityPlan]]()
  val _selectivityPlanRoots = HashMap[Int, SelectivityPlan]()
 // val _filterByEquivalence =  HashMap[Int, HashSet[FilterStatInfo]]()
  val _filterStats = HashMap[Int, FilterStatInfo]()

  def apply(plan: logical.LogicalPlan): SelectivityPlan = fromLogicPlan(plan)

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }






  private def updateFilterStats( qualifiedKeys : Seq[Seq[Expression]],
                                 targetKeys: Seq[Expression],
                                 sel : Double): Unit = {
    val filterStatInfos = getOrCreateFilters(qualifiedKeys,targetKeys)
    filterStatInfos foreach { fInfo =>
      fInfo.setSelectivity(sel)

      /*if(_filterByEquivalence.contains(fInfo.filter.semanticHash())){
        _filterByEquivalence(fInfo.filter.semanticHash()).foreach(
           f => f.setSelectivity(sel)

        )
      }*/
    }


  }

  private[this] def getOrCreateFilters( leftChildKeys : Seq[Seq[Expression]],
                                        rightKeys : Seq[Expression]) : Seq[FilterStatInfo] ={

    leftChildKeys map { keys =>
      val condition = (keys zip rightKeys).map { case (l, r) => EqualTo(l, r) }

      val semanticHash =condition.map(_.semanticHash()).sum

      _filterStats.getOrElseUpdate(semanticHash,
        if(condition.size >= 2){
          val children = condition.map(c => _filterStats.getOrElseUpdate(c.semanticHash(), FilterStatInfo(c)))
          FilterStatInfo(condition.reduceLeft(And),children)
        }
        else
           FilterStatInfo(condition.reduceLeft(And)))

    }

  }
  def isValidSelectivity(sel : Double ) : Boolean = sel >= 0 && sel <= 1
  def isValidRows(rows : Long ) : Boolean = rows >= 0

  def updatefromExecution(sparkPlan : SparkPlan) : Unit ={

    sparkPlan match {
      case join @ ShuffledHashJoin(leftKeys,rightKeys,_,_,condition,left,right) =>
        updatefromExecution(right)
        updatefromExecution(left)
        updateFilterStats(Seq(leftKeys), rightKeys,sparkPlan.selectivity())
       /* //TODO unsafe  operation
        val rows =  _selectivityPlanNodes(join.simpleHash).rows
        _selectivityPlanNodesSemantic(join.semanticHash).foreach {
          selPlan =>
            selPlan.setRowsFromExecution(rows)
            selPlan.setNumPartitionsFromExecution(sparkPlan.getNumPartitions)
        }*/
      case f@Filter(condition,child0@DataSourceScan(_, _, _, _)) =>
        _selectivityPlanNodes(f.semanticHash).setRowsFromExecution(f.getOutputRows)

      case u: UnaryNode => updatefromExecution(u.child)


      case u: LeafNode => _selectivityPlanNodes(u.semanticHash).setRowsFromExecution(u.getOutputRows)

    }

  }
  /*private[this] def updateJoin(join : SparkPlan)= SelectivityPlan ={
    val simpleHashCode = join.simpleHash
    val semanticHashCode = join.semanticHash

  }*/

  private[this] def fromLogicPlan(l: logical.LogicalPlan): SelectivityPlan = {


   l match {
      case join  @  logical.Join(left,right,_,Some(condition)) =>

        val simpleHashCode = join.simpleHash
        val semanticHashCode = join.semanticHash
        val conditions = splitConjunctivePredicates(condition)
        val filterStatInfo = _filterStats.getOrElseUpdate(conditions.map(_.semanticHash()).sum,
          if(conditions.size >= 2){
            val children = conditions.map(c => _filterStats.getOrElseUpdate(c.semanticHash(), FilterStatInfo(c)))
            FilterStatInfo(condition,children)
          }
          else{
            val   stat = FilterStatInfo(condition)
            /*if(condition.equivalencesClass.nonEmpty)
              {
                val set =_filterByEquivalence.getOrElseUpdate(condition.equivalencesClass.get.hashCode(), mutable.HashSet[FilterStatInfo]())
                set+= stat
              }*/
            stat

          }
        )

        _selectivityPlanNodes.getOrElseUpdate(simpleHashCode, {

        val newNode = HashCondition(fromLogicPlan(join.left),
            fromLogicPlan(join.right),
            filterStatInfo, simpleHashCode)
         val set=  _selectivityPlanNodesSemantic.getOrElseUpdate(semanticHashCode,HashSet[SelectivityPlan]())
            set+=newNode
          newNode
        })

      case u: logical.UnaryNode => u match {

        case f@logical.Filter(condition,  l @LogicalRelation(h: HadoopPfRelation, a, b)) =>
          val semanticHashCode = f.semanticHash
          _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
            ScanCondition(l.output, Some(condition), f.semanticHash)
          })
        case f@logical.Filter(condition,  p@logical.Project(_, _)) =>
          val semanticHashCode = f.semanticHash
          _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
            ScanCondition(p.output, Some(condition), f.semanticHash)
          })

        case _ => fromLogicPlan(u.child)
      }

      case  l @LogicalRelation(h: HadoopPfRelation, a, b) =>
        val semanticHashCode = h.semanticHash
        _selectivityPlanNodes.getOrElseUpdate(semanticHashCode, {
          ScanCondition(l.output, None , h.semanticHash)
        })



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
  private var _rows : Long = Long.MinValue
  // for future use in average
  private[sql] var numSelUpdates : Int = 0
  private[sql] var numrowsUpdates : Int = 0
  val shortName : String

  def isValid : Boolean = false;
  def selectivity : Double = Double.MinValue
  def rows : Long =  rowsFromExecution.getOrElse(_rows)

  def refresh():Unit

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


  override def equals(o: scala.Any): Boolean =  {

    throw new UnsupportedOperationException(s"$shortName does not implement equals")
  }

  /** String representation of this node without any children */
  override def simpleString: String = s"$nodeName args[$argString] rows:$rows cost:$planCost sel:$selectivity isValid:$isValid".trim
}

case class ScanCondition( outputSet : Seq[Attribute],
                          filter : Option[Expression] = None,
                          override val semanticHash: Int) extends SelectivityPlan{

  override val shortName: String = "scanCondition"
  var _isValid = false


  override def selectivity: Double = 1.0

  override def isValid : Boolean = _isValid


  override def setRows(rows : Long): Unit ={
    _isValid=true
    super.setRows(rows)
  }

  override def refresh(): Unit = {


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

    case ScanCondition(_, _,s) => /*(o zip outputSet).map{case (a,b)=> a.semanticEquals(b)}.reduceRight(_&&_) &&
      (filter zip f).map{case (a,b)=> a.semanticEquals(b)}.reduceRight(_&&_)*/
     s==semanticHash
    case _ => false

  }
}

case class HashCondition(left : SelectivityPlan,
                         right : SelectivityPlan,
                         condition : FilterStatInfo,
                         override val  semanticHash : Int) extends SelectivityPlan{

  override val shortName: String = "hashCondition"
  private var optimizedSelectivity: Option[Double] = None


  override def isValid : Boolean = (selectivity <= 1 && selectivity >= 0) &&
    rows >= 1  &&
    planCost() >= 0 &&
    left.isValid && right.isValid


  override def refresh(): Unit = {


  }

  override def selectivity: Double = {
    if(rowsFromExecution.isDefined && children.map(_.isValid).reduceLeft(_&&_)){
      val sel = rowsFromExecution.get.asInstanceOf[Double]/ children.map(_.rows).product
      if(condition.selectivity < 0 )
        condition.setSelectivity(sel)
      sel
    }
    else
    condition.selectivity
  }

  override def rows: Long =   rowsFromExecution.getOrElse(children.map(_.rows).foldLeft(selectivity)(_*_).toLong)




  override def equals(o: scala.Any): Boolean =  o match {

      case HashCondition(l, r, c,_) => c.equals(condition)  && l.equals(left) && r.equals(right)

      case _ => false

    }
  override def planCost(): Double = {

    var startup_cost = right.planCost + left.planCost
    startup_cost += (CPU_OPERATOR_COST  + CPU_TUPLE_COST) * right.rows
    var run_cost = HASH_QUAL_COST * left.rows * right.rows * 0.75
    run_cost += CPU_OPERATOR_COST * left.rows
    run_cost += (CPU_TUPLE_COST + 1 )* rows
    startup_cost + run_cost

  }
  /*override def toString: String =  {

    super.toString + s"$shortName${condition.toString}${left.toString}${right.toString}"
  }*/




  override def children: Seq[SelectivityPlan] = Seq(left) ++ Seq(right)


}

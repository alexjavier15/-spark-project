package org.apache.spark.sql.execution

import javax.sound.sampled.FloatControl.Type

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.{ExperimentalMethods, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EquivalencesClass, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{MJoinGeneralOptimizer, MJoinOrderingOptimizer, MJoinPushPredicateThroughJoin, Optimizer}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HolderLogicalRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.pf.HadoopPfRelation

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.reflect.ClassTag

/**
  * Created by alex on 21.03.16.
  */

object JoinOptimizer{
  val gOptimizer = new MJoinGeneralOptimizer
  var joinOptimizer : JoinOptimizer = null

  def apply(originPlan: LogicalPlan, sqlContext: SQLContext) : JoinOptimizer ={
    if(joinOptimizer==null || joinOptimizer.originPlan!=originPlan)
      joinOptimizer =new JoinOptimizer(gOptimizer.execute(originPlan),sqlContext)
    joinOptimizer
  }



  def findRootJoin(plan: LogicalPlan): LogicalPlan = {

    plan match {

      case j@Join(_, _, _, _) => j
      case node: logical.UnaryNode => findRootJoin(node.child)
      case _ => throw new IllegalArgumentException("Only UnaryNode must be above a Join")
    }

  }

}

class JoinOptimizer(private val originPlan: LogicalPlan, val sqlContext: SQLContext) extends PredicateHelper with Logging{
  private var originalOrder : Option[LogicalPlan] = None

  val chunkedRels = findChunkedBaseRelPlans()
  val _trainingSet = HashMap[Int,LogicalPlan]()

  lazy val mJoinLogical: Option[LogicalPlan] = analyzeWithMJoin(doPermutations(baseRelations))
  lazy private val (baseRelations, originalConditions): (Seq[LogicalPlan], Seq[Expression]) =
    extractJoins(originPlan,splitConjunctivePredicates).
      getOrElse((Seq(), Seq()))

  lazy private val _otherConditions= HashSet[Expression]()
  lazy private val eqClasses = HashSet[EquivalencesClass]()
  private val all_joins  = HashMap[Int, ListBuffer[LogicalPlan]]()



  private def findChunkedBaseRelPlans(): Seq[(LogicalPlan, Seq[LogicalPlan])] = {
    if(!sqlContext.conf.mJoinEnabled)
      Seq.empty

    //  Map every baseRealtion with its HadoopPfRelation
    val sources = baseRelations.map {
      case relation @LogicalRelation(h: HadoopPfRelation, _, _) =>
        (relation, Some(h))
      case relation => (relation, None)
    }



    // fore very  HadoopPfRelation found build relation for every chunk

 sources.map(plan => (plan._1, splitRelation(plan._1, plan._2)))


  }

  private def splitRelation(plan: LogicalPlan, relation: Option[HadoopPfRelation]): Seq[LogicalPlan] = {


   val res = relation match {
      case None => Seq(plan)
      case Some(h) =>
        val splittedHPFRelation = h.splitHadoopPfRelation()

        splittedHPFRelation.map(newRel => {
       plan transform {
            case l@LogicalRelation(h: HadoopPfRelation, a, b) =>

              LogicalRelation(newRel, Some(l.output), b)
          }
       }
          )
    }

    assert(res.forall(r => r.semanticHash == plan.semanticHash) )
    res
  }

  /**
    * Returns a tuple with containing a Seq of child plans and Seq of join
    * conditions
    *
    */
  private def extractJoins(plan: LogicalPlan , f : Expression => Seq[Expression]): Option[(Seq[LogicalPlan], Seq[Expression])] = {
    if(!sqlContext.conf.mJoinEnabled )
      Seq.empty

    // flatten all inner joins, which are next to each other
    def flattenJoin(plan: LogicalPlan,f : Expression => Seq[Expression]): (Seq[LogicalPlan], Seq[Expression]) = plan match {
      case logical.Join(left, right, Inner, cond) =>
        val (plans, conditions) = flattenJoin(left,f)
        (plans ++ Seq(right), conditions ++ cond.toSeq)

      case logical.Filter(filterCondition, j@Join(left, right, Inner, joinCondition)) =>
        val (plans, conditions) = flattenJoin(j,f)
        (plans, conditions ++ f(filterCondition))
      case logical.Project(fields, child) => flattenJoin(child,f)
      case logical.Aggregate(a, b, child) =>
        flattenJoin(child,f)
      case logical.GlobalLimit(_, child) => flattenJoin(child,f)
      case logical.LocalLimit(_, child) => flattenJoin(child,f)
      case logical.Sort(_,_,child)=>flattenJoin(child,f)
      case _ => (Seq(plan), Seq())
    }

    plan match {
      case fil @logical.Filter(filterCondition, j@Join(_, _, Inner, _)) =>
        Some(flattenJoin(fil,f))
      case j@logical.Join(_, _, Inner, _) =>
        Some(flattenJoin(j,f))
      case p@logical.Project(fields, child) =>
        Some(flattenJoin(child,f))
      case o@logical.Aggregate(_, _, child) =>
        Some(flattenJoin(o,f))
      case l1@logical.GlobalLimit(_, child) => Some(flattenJoin(l1,f))
      case l2@logical.LocalLimit(_, child) => Some(flattenJoin(l2,f))
      case _ => None
    }


  }


  private def addEquivMembers(condition: Expression): Unit = {

    val eqClassNew = new EquivalencesClass

    eqClassNew.addEquivalence(condition)
    //not a join condition
    if( eqClassNew.members.isEmpty){
      _otherConditions+=condition
    }else{

      logDebug("Adding  new equivalence classes for :"+condition.toString)
      val mergedClasses =eqClasses.foldLeft(eqClassNew)(_.mergeWith(_))
      logDebug("Merged  result :"+mergedClasses.toString)

      eqClasses.filter(_.canMergeWith(mergedClasses)).foreach {
        eqclass =>
        logDebug("Deleting   equivalence classes  :"+eqclass.toString +" merged with "+ mergedClasses.toString)
        eqClasses -= eqclass
      }

      eqClasses+=mergedClasses
    }
  }



  private def combinePredicates(conditions: List[Expression]): Expression = {
    conditions match {
      case cond1 :: cond2 :: Nil => And(cond1, cond2)
      case cond :: xs => And(cond, combinePredicates(xs))
      case Nil => null
    }
  }

  private def createOrderedJoin(input: Seq[LogicalPlan],
                                conditions: Set[Expression]): LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      Join(input(0), input(1), Inner, conditions.reduceLeftOption(And))
    } else {
      val left :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { plan =>
        val refs = left.outputSet ++ plan.outputSet
        conditions.filterNot(canEvaluate(_, left)).filterNot(canEvaluate(_, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val right = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(_.references.subsetOf(joinedRefs))
      val joined = logical.Join(left, right, Inner, joinConditions.reduceLeftOption(And))

      // should not have reference to same logical plan
      createOrderedJoin(Seq(joined) ++ rest.filterNot(_ eq right), others)

    }

  }


  private def withHolders(logicalPlan: LogicalPlan): LogicalPlan ={

   logicalPlan transformUp {

       case l @ LogicalRelation(h: HadoopPfRelation, a, b) =>

         HolderLogicalRelation(l,h);


   }

  }



  private def haveEquivalentOrder(plan1 :Seq[LogicalPlan],plan2 :Seq[LogicalPlan] ): Boolean ={
    assert(plan1.size== plan2.size  )
    if(plan1.size == 2)
      true
    else{
      val testSize = plan1.size -2
      (plan1.take(testSize) zip plan2.take(testSize)).forall{

        case (a,b) => a.semanticHash == b.semanticHash
      }

    }
  }
  private def analyzeWithMJoin(inferedPlans: List[(LogicalPlan,
    List[Expression])]): Option[LogicalPlan] = {

    if(!sqlContext.conf.mJoinEnabled )
      List.empty

    if (inferedPlans == Nil) {
      None
    }
    else {


      println("infered plans : " + inferedPlans.size)

      val trainingJoins = _trainingSet.values.map(
        join =>   {
          def appendFilter(join : LogicalPlan): LogicalPlan ={
            val filters = _otherConditions.toSeq.filter( c => c.references.subsetOf(join.outputSet))
            if(filters.nonEmpty){

              val condition = filters .reduceLeft(And)
              logical.Filter(condition,join)
            }else
              join

          }

          val analyzed = sqlContext.sessionState.analyzer
          .execute(Aggregate(Seq(),Seq(Count(Literal(1)).toAggregateExpression(false)).map(UnresolvedAlias(_)),appendFilter(join)))
          sqlContext.sessionState.optimizer.execute(analyzed)


        }


        ).toSeq
      logInfo(trainingJoins.toString)

      val analyzedJoins = inferedPlans.map(subplan => optimizeSubplan(subplan._1, subplan._2))


      val analyzedChunkedRelations: Seq[(LogicalPlan, Seq[LogicalPlan])] = chunkedRels.map(relations =>
        (optimizePlans(Seq(relations._1)).head, optimizePlans(relations._2)))
      val toExecute = originPlan


      logInfo(toExecute.toString)
      val optimzedOriginal = sqlContext.sessionState.optimizer.execute(toExecute)



      logInfo(optimzedOriginal.toString)

      val rootJoin = JoinOptimizer.findRootJoin(optimzedOriginal)

      val mJoin = logical.MJoin(rootJoin, analyzedChunkedRelations, Some(analyzedJoins.::(rootJoin)), trainingJoins)

      val res = appendPlan[logical.Join](optimzedOriginal, mJoin)


      Some(res)




    }

  }

  private def optimizePlans( plans : Seq[LogicalPlan]): Seq[LogicalPlan] ={

      //val analyzed : Seq[LogicalPlan] =  plans map sqlContext.sessionState.analyzer.execute
     // plans foreach sqlContext.sessionState.analyzer.checkAnalysis

    //plans map sqlContext.sessionState.optimizer.execute
    plans map sqlContext.sessionState.optimizer.execute

  }

  private[sql] def convertToChunkedJoinPlan(logicalPlans: Seq[LogicalPlan],
                                            dict: Seq[(LogicalPlan,
                                              Seq[LogicalPlan])]): Seq[LogicalPlan] = {

    def replaceWith(logicalPlan: LogicalPlan, substitution: LogicalPlan): LogicalPlan = {

      substitution match {
        case logical.SubqueryAlias(_, l@LogicalRelation(h: HadoopPfRelation, _, _)) =>
          logicalPlan transform {
            case l@LogicalRelation(parent: HadoopPfRelation, _, _) if h.isChild(parent) =>
              substitution
          }
        case _ => logicalPlan

      }
    }

    def replaceAllWith(plans: Seq[LogicalPlan], substitution: LogicalPlan): Seq[LogicalPlan] =
      plans.map(plan => replaceWith(plan, substitution))


    if (dict == Nil || dict.isEmpty)
      logicalPlans
    else {
      val (origin, substitutions) = (dict.head._1, dict.head._2)

      val substituted = substitutions.flatMap(substitution => replaceAllWith(logicalPlans, substitution))
      convertToChunkedJoinPlan(substituted, dict.tail)
    }
  }

  private def optimizeSubplan(joined: LogicalPlan, conditions: Seq[Expression]): LogicalPlan = {


    val otherFilters = _otherConditions.reduceLeftOption(And)


    otherFilters match {

      case Some(f) =>
        val filter = logical.Filter(otherFilters.get, joined)
        appendPlan[logical.Filter](originPlan, filter)
      case None =>
        appendPlan[logical.Join](originPlan, joined)
    }


  }

  private def appendPlan[T: ClassTag](root: LogicalPlan, plan: LogicalPlan): LogicalPlan = {

    root match {
      case j: T => plan
      case node: logical.UnaryNode =>
        node.withNewChildren(Seq(appendPlan(node.child, plan)))
      case others => throw new IllegalArgumentException("Not found :" + plan.nodeName + " Only UnaryNode must be above a Join " + others.nodeName)
    }
  }


  private def initEquivClasses: Unit = {

    originalConditions.foreach(cond => addEquivMembers(cond))


  }

  private def  doPermutations(plans: Seq[LogicalPlan]): List[(LogicalPlan,
    List[Expression])] = {
    if (plans != Nil) {
      logDebug("****Extracted Conditions*****")
      logDebug(originalConditions.toString())
      // anylyzed the reordered joins
      initEquivClasses
      logInfo("*** Inititialized equivalence classes ***")
      logInfo(eqClasses.toString())
      val permutations = permute(plans.toList.reverse)
      logInfo("*** End equivalence classes ***")
      logInfo(eqClasses.toString())
      logInfo("*** othr  Conditions ***")
      logInfo(_otherConditions.toString())

      permutations

    }
    else {
      Nil
    }

  }


  private def createJoinCondition(left: LogicalPlan, right: LogicalPlan): Set[Expression] = {


    val joinConditions = eqClasses.map(eqClass =>
      eqClass.generateJoinImpliedEqualities(left, right)
    ).filter(_.isDefined).map(_.get).toSet
    joinConditions
  }


  /** For each element x in List xss, returns (x, xss - x) */
  private[this] def selections[A](xss: List[A]): List[(A, List[A])] = xss match {
    case Nil => Nil
    case x :: xs =>
      (x, xs) :: (for ((y, ys) <- selections(xs))
        yield (y, x :: ys))
  }

  /** Returns a list containing all permutations of the input list */
 def permute(xs: List[LogicalPlan]): List[(LogicalPlan,
    List[Expression])] = xs match {
    case Nil =>Nil

    // special case lists of length 1 and 2 for better performance
    case t :: Nil => List((t, Nil))
    case t :: u :: Nil =>
      val conditions1 = createJoinCondition(t, u)
      val conditions2 = createJoinCondition(u, t)

      if (conditions1.nonEmpty && conditions2.nonEmpty) {
        val join = createOrderedJoin(Seq(t, u), conditions1)
        _trainingSet+= join.semanticHash -> join
        List((join, conditions1.toList))
      } else
        Nil

    case _ =>
      for ((y, ys) <- selections(xs); ps <- permute(ys) ;conditions = createJoinCondition(ps._1, y ) if conditions.nonEmpty)
        yield (createOrderedJoin(Seq(ps._1,y),conditions ), ps._2 ++ conditions)

  }


















  //private[this] def CreateOptimizedReorderedPlan(plan : LogicalPlan , targetPlan : LogicalPlan): LogicalPlan = {
//
  //}

}

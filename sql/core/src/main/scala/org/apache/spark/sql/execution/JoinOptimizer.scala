package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, EquivalencesClass, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{MJjoinColumnPrunning, MJoinGeneralOptimizer}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, logical}
import org.apache.spark.sql.execution.datasources.pf.HadoopPfRelation
import org.apache.spark.sql.execution.datasources.{HolderLogicalRelation, LogicalRelation}

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.reflect.ClassTag

/**
  * Created by alex on 21.03.16.
  */

object JoinOptimizer {
  val gOptimizer = new MJoinGeneralOptimizer
  var joinOptimizer: JoinOptimizer = null

  def apply(originPlan: LogicalPlan, sqlContext: SQLContext): JoinOptimizer = {
    println("Executing MJOIN")
    if (joinOptimizer == null || joinOptimizer.originPlan != originPlan)
      joinOptimizer = new JoinOptimizer(gOptimizer.execute(originPlan), sqlContext)
    joinOptimizer
  }

  def DummyCountDistinct: SparkPlan = {

    null
  }



}

class JoinOptimizer(private val originPlan: LogicalPlan, val sqlContext: SQLContext) extends PredicateHelper with Logging {
  lazy val mJoinLogical: Option[LogicalPlan] = analyzeWithMJoin()
  lazy val columnStatPlans = buildColumnStatPlans()
  lazy val joinAlternatives = buildJoinAlternatives()
  val projectionOptimizer = new MJjoinColumnPrunning
  lazy private val (baseRelations, originalConditions): (Seq[LogicalPlan], Seq[Expression]) =
    extractJoins(splitConjunctivePredicates).
      getOrElse((Seq(), Seq()))
  lazy private val _otherConditions = HashSet[Expression]()
  lazy private val eqClasses = HashSet[EquivalencesClass]()
  lazy val chunkedRels = findChunkedBaseRelPlans()
  lazy val inferedPlans = doPermutations()
  private val all_joins = HashMap[Int, ListBuffer[LogicalPlan]]()
  private var originalOrder: Option[LogicalPlan] = None

  /** Returns a list containing all permutations of the input list */
  private def permute(xs: List[LogicalPlan]): List[(LogicalPlan,
    List[Expression])] = xs match {
    case Nil => Nil

    // special case lists of length 1 and 2 for better performance
    case t :: Nil => List((t, Nil))
    case t :: u :: Nil =>
      val conditions1 = createJoinCondition(t, u)
      val conditions2 = createJoinCondition(u, t)

      if (conditions1.nonEmpty && conditions2.nonEmpty) {
        val join = createOrderedJoin(Seq(t, u), conditions1)
        List((join, conditions1.toList))
      } else
        Nil

    case _ =>
      for ((y, ys) <- selections(xs); ps <- permute(ys); conditions = createJoinCondition(ps._1, y) if conditions.nonEmpty)
        yield (createOrderedJoin(Seq(ps._1, y), conditions), ps._2 ++ conditions)

  }

  /**
    * Returns a tuple with containing a Seq of child plans and Seq of join
    * conditions
    *
    */
  private def extractJoins(f: Expression => Seq[Expression]): Option[(Seq[LogicalPlan], Seq[Expression])] = {
    if (!sqlContext.conf.mJoinEnabled)
      Seq.empty

    // flatten all inner joins, which are next to each other
    def flattenJoin(plan: LogicalPlan, f: Expression => Seq[Expression]): (Seq[LogicalPlan], Seq[Expression]) = plan match {
      case logical.Join(left, right, Inner, cond) =>
        val (plans, conditions) = flattenJoin(left, f)
        (plans ++ Seq(right), conditions ++ cond.toSeq)

      case logical.Filter(filterCondition, j@Join(left, right, Inner, joinCondition)) =>
        val (plans, conditions) = flattenJoin(j, f)
        (plans, conditions ++ f(filterCondition))
      case node : logical.UnaryNode  => flattenJoin(node.child, f)
      case _ => (Seq(plan), Seq())
    }

    originPlan match {
      case fil@logical.Filter(filterCondition, j@Join(_, _, Inner, _)) =>
        Some(flattenJoin(fil, f))
      case j@logical.Join(_, _, Inner, _) =>
        Some(flattenJoin(j, f))
      case node : logical.UnaryNode => Some(flattenJoin(node, f))
      case _ => None
    }


  }

  private def addEquivMembers(condition: Expression): Unit = {

    val eqClassNew = new EquivalencesClass

    eqClassNew.addEquivalence(condition)
    //not a join condition
    if (eqClassNew.members.isEmpty) {
      _otherConditions += condition
    } else {

      logDebug("Adding  new equivalence classes for :" + condition.toString)
      val mergedClasses = eqClasses.foldLeft(eqClassNew)(_.mergeWith(_))
      logDebug("Merged  result :" + mergedClasses.toString)

      eqClasses.filter(_.canMergeWith(mergedClasses)).foreach {
        eqclass =>
          logDebug("Deleting   equivalence classes  :" + eqclass.toString + " merged with " + mergedClasses.toString)
          eqClasses -= eqclass
      }

      eqClasses += mergedClasses
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

  private def withHolders(logicalPlan: LogicalPlan): LogicalPlan = {

    logicalPlan transformUp {

      case l@LogicalRelation(h: HadoopPfRelation, a, b) =>

        HolderLogicalRelation(l, h);


    }

  }

  private def haveEquivalentOrder(plan1: Seq[LogicalPlan], plan2: Seq[LogicalPlan]): Boolean = {
    assert(plan1.size == plan2.size)
    if (plan1.size == 2)
      true
    else {
      val testSize = plan1.size - 2
      (plan1.take(testSize) zip plan2.take(testSize)).forall {

        case (a, b) => a.semanticHash == b.semanticHash
      }

    }
  }

  private def analyzeWithMJoin(): Option[LogicalPlan] = {


      val optimizedOriginal = sqlContext.sessionState.optimizer.execute(originPlan)
      val rootJoin = findRootJoin(optimizedOriginal)
      if(rootJoin != null){
      val mJoin = logical.MJoin(rootJoin)
      val res = appendPlan(optimizedOriginal, mJoin)
        Some(res)
      }
    else{
        Some(optimizedOriginal)
      }


  }

  private def optimizePlans(plans: Seq[LogicalPlan]): Seq[LogicalPlan] = plans map sqlContext.sessionState.optimizer.execute

  private def convertToChunkedJoinPlan(logicalPlans: Seq[LogicalPlan],
                                       dict: Seq[(LogicalPlan,
                                         Seq[LogicalPlan])]): Seq[LogicalPlan] = {

    def replaceAllWith(plans: Seq[LogicalPlan], substitution: LogicalPlan): Seq[LogicalPlan] =
      plans.map(plan => replaceWith(plan, substitution))

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

    val subplan =otherFilters match {
      case Some(f) =>
        val filter = logical.Filter(otherFilters.get, joined)
        appendPlanFilter(originPlan, filter)
      case None =>
        appendPlan(originPlan, joined)
    }

    projectionOptimizer.execute(subplan)

  }

  private def appendPlanFilter(root: LogicalPlan, plan: LogicalPlan): LogicalPlan = {
    root match {
      case j @ logical.Filter(_,_)  =>
        plan
      case node: logical.UnaryNode =>
        node.withNewChildren(Seq(appendPlanFilter(node.child, plan)))
      case others => throw new IllegalArgumentException("Not found :" + plan.nodeName + " Only UnaryNode must be above a Join " + others.nodeName)
    }

  }



  private def appendPlan(root: LogicalPlan, plan: LogicalPlan): LogicalPlan = {


    root match {
      case j: logical.Join  => plan
      case node: logical.UnaryNode =>
        node.withNewChildren(Seq(appendPlan(node.child, plan)))
      case others => throw new IllegalArgumentException("Not found :" + plan.nodeName + " Only UnaryNode must be above a Join " + others.nodeName)
    }
  }

  private def initEquivClasses() = originalConditions.foreach(cond => addEquivMembers(cond))

  private def doPermutations() = {
    if (baseRelations != Nil) {
      logDebug("****Extracted Conditions*****")
      logDebug(originalConditions.toString())
      // anylyzed the reordered joins
      initEquivClasses()
      logInfo("*** Inititialized equivalence classes ***")
      logInfo(eqClasses.toString())
      val permutations = permute(baseRelations.toList.reverse)
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

  private def findChunkedBaseRelPlans() = {
      //  Map every baseRealtion with its HadoopPfRelation
      val sources = baseRelations.map {
        case relation@LogicalRelation(h: HadoopPfRelation, _, _) =>
          (relation, Some(h))
        case relation => (relation, None)
      }
      sources.map {
        case (parentPlan, baseRelation) =>
          parentPlan.simpleHash -> splitRelation(parentPlan, baseRelation)
      }.toMap
  }

  private def splitRelation(plan: LogicalPlan, relation: Option[HadoopPfRelation]): Seq[LogicalPlan] = {
    val res = relation match {
      case None => Seq(plan)
      case Some(h) =>
        val splittedHPFRelation = h.splitHadoopPfRelation()

        splittedHPFRelation.map { newRel =>
          plan transform {
            case l@LogicalRelation(h: HadoopPfRelation, a, b) =>
              LogicalRelation(newRel, Some(l.output), b)
          }
        }
    }
    assert(res.forall(r => r.semanticHash == plan.semanticHash))
    res
  }

  private def buildJoinAlternatives() = inferedPlans.map { case (plan, conditions) => optimizeSubplan(plan, conditions) }

  private def buildColumnStatPlans() = {

    def pruneFilters(plan: LogicalPlan) = {
      val condition = _otherConditions.toSeq.
        filter(c => c.references.subsetOf(plan.outputSet))

      val newChild = condition match {
        case Seq() => plan
        case c => logical.Filter(condition.reduceLeft(And), plan)
      }
      newChild
    }

    def createProjection(projectList: Seq[NamedExpression], plan: LogicalPlan) = {
      projectList.map { p =>
        val analyzed = sqlContext.sessionState.analyzer
          .execute(logical.Distinct(logical.Project(Seq(p),  pruneFilters(plan))))

        p -> sqlContext.sessionState.optimizer.execute(analyzed)
      }
    }

    baseRelations.flatMap(
      plan => {
        val projectList = eqClasses.flatMap {
          eqc => eqc.members
            .map(m => m.outputSet.references)
            .filter(r => r.subsetOf(plan.outputSet))
            .map(s => s.head)
        }.toSeq

        createProjection(projectList,plan)
      }

    ).toMap
  }

  private def findRootJoin(plan: LogicalPlan): LogicalPlan = {

    plan match {

      case j@Join(_, _, _, _) => j
      case node: logical.UnaryNode => findRootJoin(node.child)
      case _ => null
    }

  }
}

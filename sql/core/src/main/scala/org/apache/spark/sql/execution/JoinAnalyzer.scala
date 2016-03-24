package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

import scala.collection.mutable

/**
  * Created by alex on 21.03.16.
  */
class JoinAnalyzer(private val originPlan: LogicalPlan, val sqlContext: SQLContext) extends PredicateHelper {

  lazy private val (baseRelations, originalConditions): (Seq[LogicalPlan], Seq[Expression]) =
    extractJoins(originPlan).
      getOrElse((Seq(), Seq()))
  lazy private val equivClasses = new EquivalencesClass
  lazy private val eqClasses: scala.collection.mutable.Set[EquivalencesClass] = mutable.Set()
  lazy private val accumulator: mutable.Seq[LogicalPlan] = mutable.Seq()
  val mJoinLogical: Option[LogicalPlan] = analyzeWithMJoin(doPermutations(baseRelations))
  private val all_joins: mutable.Map[Int, mutable.ListBuffer[LogicalPlan]] = mutable.Map()

  /**
    * Returns a tuple with containing a Seq of child plans and Seq of join
    * conditions
    *
    */
  private def extractJoins(plan: LogicalPlan): Option[(Seq[LogicalPlan], Seq[Expression])] = {

    // flatten all inner joins, which are next to each other
    def flattenJoin(plan: LogicalPlan): (Seq[LogicalPlan], Seq[Expression]) = plan match {
      case logical.Join(left, right, Inner, cond) =>
        val (plans, conditions) = flattenJoin(left)
        (plans ++ Seq(right), conditions ++ cond.toSeq)

      case logical.Filter(filterCondition, j@Join(left, right, Inner, joinCondition)) =>
        val (plans, conditions) = flattenJoin(j)
        (plans, conditions ++ splitConjunctivePredicates(filterCondition))
      case logical.Project(fields, child) => flattenJoin(child)
      case _ => (Seq(plan), Seq())
    }
    plan match {
      case f@logical.Filter(filterCondition, j@Join(_, _, Inner, _)) =>
        Some(flattenJoin(f))
      case j@logical.Join(_, _, Inner, _) =>
        Some(flattenJoin(j))
      case p@logical.Project(fields, child) =>
        accumulator :+ p
        Some(flattenJoin(child))

      case _ => None
    }


  }


  private def addEquivMembers(children: Seq[LogicalPlan], condition: Expression): Seq[LogicalPlan] = {
    equivClasses.addEquivalence(condition, children)
    children
  }


  private def createJoin(left: Seq[LogicalPlan], right: Seq[LogicalPlan]): Option[Expression] = {

    equivClasses.generateJoinImpliedEqualities(left.toSet, right.toSet)


  }

  private def combinePredicates(conditions: List[Expression]): Expression = {
    conditions match {
      case cond1 :: cond2 :: Nil => And(cond1, cond2)
      case cond :: xs => And(cond, combinePredicates(xs))
    }
  }

  private def createOrderedJoin(input: Seq[LogicalPlan],
                                conditions: Seq[Expression]): LogicalPlan = {

    if (input.size == 2) {
      val condition = conditions.reduceLeftOption(And)
      logical.Join(input.head, input(1), Inner, None)

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
      val joined = logical.Join(left, right, Inner, None)

      // should not have reference to same logical plan
      createOrderedJoin(Seq(joined) ++ rest.filterNot(_ eq right), others)

    }

  }

  private def analyzeWithMJoin(inferedPlans: List[(List[LogicalPlan],
    List[Option[Expression]])]): Option[LogicalPlan] = {

    if (inferedPlans == Nil) {
      None
    }
    else {
      val resolved = inferedPlans.map(x => (x._1, x._2.map(expr => expr.orNull)))
      val filterPlans = resolved.filter(x => !x._2.contains(null))
      val analyzedJoins = filterPlans.map(subplan => analyzeSubplan(subplan._1, subplan._2))
      // We can gather one for all the leaves relations (we assume unique projections and
      // filter. Plan the original plan and get the result
      val analyzedOriginal = sqlContext.sessionState.optimizer.execute(originPlan)
      val (leaves, filters) = extractJoins(analyzedOriginal).getOrElse((Seq(),Seq()))

      //Extract the first Join. To-Do : Mjoin must not have a join as child we do that to
      // test execution of at least one plan

      val mJoin = logical.MJoin(findRootJoin(analyzedOriginal), leaves, Some(analyzedJoins))

      // make a copy of all plans above the root join and append the MJoin plan
       Some(appendPlan(analyzedOriginal,mJoin))


    }

  }

  private def analyzeSubplan(plans0: Seq[LogicalPlan], conditions: Seq[Expression]): LogicalPlan = {

    // Create join
    val joined = createOrderedJoin(plans0, conditions)
    // reduce join conditions to Filters
    val filter = logical.Filter(conditions.reduceLeftOption(And).get, joined)
    // Add projection
    val dummy_plan = originPlan.withNewChildren(Seq(filter))

    // Analyze
    val analyzedPlan = sqlContext.sessionState.optimizer.execute(dummy_plan)
    // return the Join root plan (remove filter and projection nodes as they are the same
    // for each subplan
    findRootJoin(analyzedPlan)

  }

  private def appendPlan(root: LogicalPlan, plan: LogicalPlan): LogicalPlan = {
    plan match {
      case m @ logical.MJoin(_,_,_) => m
      case j@logical.Join(_, _, _, _) => root.withNewChildren(Seq(plan))
      case node: logical.UnaryNode => node.withNewChildren(Seq(appendPlan(node.child, plan)))
      case _ => throw new IllegalArgumentException("Only UnaryNode must be above a Join")
    }
  }

  private def findRootJoin(plan: LogicalPlan): LogicalPlan = {

    plan match {

      case j@Join(_, _, _, _) => j
      case node: logical.UnaryNode => findRootJoin(node.child)
      case _ => throw new IllegalArgumentException("Only UnaryNode must be above a Join")
    }

  }

  private def initEquivClasses: Unit = {

    originalConditions.foreach(cond => addEquivMembers(baseRelations, cond))


  }

  private def doPermutations(plans: Seq[LogicalPlan]): List[(List[LogicalPlan],
    List[Option[Expression]])] = {
    if (plans != Nil) {
      initEquivClasses
      permute(plans.toList.reverse)
    }
    else {
      Nil
    }

  }


  /** For each element x in List xss, returns (x, xss - x) */
  private[this] def selections[A](xss: List[A]): List[(A, List[A])] = xss match {
    case Nil => Nil
    case x :: xs =>
      (x, xs) :: (for ((y, ys) <- selections(xs))
        yield (y, x :: ys))
  }

  /** Returns a list containing all permutations of the input list */
  private[this] def permute(xs: List[LogicalPlan]): List[(List[LogicalPlan],
    List[Option[Expression]])] = xs match {
    case Nil => List((Nil, Nil))

    // special case lists of length 1 and 2 for better performance
    case t :: Nil => List((xs, Nil))
    case t :: u :: Nil =>
      val r_condition = createJoin(Seq(t), Seq(u))
      val l_condition = createJoin(Seq(u), Seq(t))
      List((xs, List(r_condition)), (List(u, t), List(l_condition)))

    case _ =>
      for ((y, ys) <- selections(xs); ps <- permute(ys))
      //   yield (y :: ps._1, inferJoinClauseEquiv1(y, ps._2.head) :: ps._2)
        yield (ps._1 :+ y, ps._2 :+ createJoin(ps._1, Seq(y)))
  }


}

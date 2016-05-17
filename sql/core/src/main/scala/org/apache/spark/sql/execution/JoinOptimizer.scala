package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ExperimentalMethods, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{MJoinGeneralOptimizer, MJoinOrderingOptimizer, MJoinPushPredicateThroughJoin, Optimizer}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{HolderLogicalRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.pf.HadoopPfRelation

import scala.collection.mutable
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
  lazy private val (baseRelations, originalConditions): (Seq[LogicalPlan], Seq[Expression]) =
    extractJoins(originPlan,splitConjunctivePredicates).
      getOrElse((Seq(), Seq()))

  lazy private val _otherConditions= mutable.Set[Expression]()
  lazy private val eqClasses: scala.collection.mutable.Set[EquivalencesClass] = mutable.Set()
  lazy private val accumulator: mutable.Seq[LogicalPlan] = mutable.Seq()
  val chunkedRels = findChunkedBaseRelPlans()
  val mJoinLogical: Option[LogicalPlan] = analyzeWithMJoin(doPermutations(baseRelations))

  private val all_joins: mutable.Map[Int, mutable.ListBuffer[LogicalPlan]] = mutable.Map()



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
      case logical.Aggregate(_, _, child) => flattenJoin(child,f)
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
        accumulator :+ p
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


  private def createJoinCondition(left: Seq[LogicalPlan], right: Seq[LogicalPlan]): Seq[Expression] = {


    val joinConditions = eqClasses.flatMap(eqClass =>
      eqClass.generateJoinImpliedEqualities(left.toSet, right.toSet).headOption
      ).toSeq


    joinConditions

  }

  private def combinePredicates(conditions: List[Expression]): Expression = {
    conditions match {
      case cond1 :: cond2 :: Nil => And(cond1, cond2)
      case cond :: xs => And(cond, combinePredicates(xs))
    }
  }

  private def createOrderedJoin(input: Seq[LogicalPlan],
                                conditions: Seq[Expression]): LogicalPlan = {
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


      // transform by remplacing LogicalRelations by  HolderLogicalRelations (Saving plannign time)
      /*
      * TO-DO
      * */
      originalOrder=None
      println("infered plans : "+ inferedPlans.size)


      /*val grouped = inferedPlans.groupBy(subplan => {

        extractJoins(subplan._1, x => Seq(x)) match {
          case  Some( (r : Seq[LogicalPlan] ,c : Seq[Expression]) ) =>
            if(haveEquivalentOrder(baseRelations,r))
              originalOrder=Some(subplan._1)
            c.map(x => x.semanticHash()).sum
          case _ => -1
        }


      })

      val  counted = grouped.map( x => x._1 -> x._2.size)

     val max = counted.maxBy{
        case (hash,count) => count
      }
      logInfo(counted.toString())

      logInfo("filtered plans size: "+ max._2)*/



      val analyzedJoins = inferedPlans.map(subplan => optimizeSubplan(subplan._1, subplan._2))
      // We can gather one for all the leaves relations (we assume unique projections and
      // filter. Plan the original plan and get the result
     // val analyzedOriginal = optimizePlans(Seq(originPlan)).head
      //val (leaves, filters) = extractJoins(analyzedOriginal).getOrElse((Seq(), Seq()))
      //logInfo(chunkedRels.toString())

      //Extract the first Join. To-Do : Mjoin must not have a join as child we do that to
      // test execution of at least one plan

      val analyzedChunkedRelations: Seq[(LogicalPlan, Seq[LogicalPlan])] = chunkedRels.map(relations =>
        ( optimizePlans(Seq(relations._1)).head ,optimizePlans(relations._2)))
      //Experimental:
      val toExecute =originPlan
      /*originalOrder match{

        case Some(p) => p
        case None =>
          logInfo("Not found original Order. Getting random")
          val r = scala.util.RandomoriginPlan
          val planId =  r.nextInt(analyzedJoins.size)
          analyzedJoins(planId)
      }*/


      logInfo(toExecute.toString)
      val optimzedOriginal =sqlContext.sessionState.optimizer.execute(toExecute)



      logInfo(optimzedOriginal.toString)

      val rootJoin = JoinOptimizer.findRootJoin(optimzedOriginal)
    //  val union = sqlContext.sessionState.analyzer.execute(logical.Union(chunkedOriginalJoin))
    //  val analyzedUnion = sqlContext.sessionState.optimizer.execute(union)
      val mJoin = logical.MJoin(rootJoin, analyzedChunkedRelations, Some(analyzedJoins.::(rootJoin)))



      // make a copy of all plans above the root join and append the MJoin plan
     val res =appendPlan[logical.Join](optimzedOriginal, mJoin)


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


  /** For each element x in List xss, returns (x, xss - x) */
  private[this] def selections[A](xss: List[A]): List[(A, List[A])] = xss match {
    case Nil => Nil
    case x :: xs =>
      (x, xs) :: (for ((y, ys) <- selections(xs))
        yield (y, x :: ys))
  }

  /** Returns a list containing all permutations of the input list */
  private[this] def permute(xs: List[LogicalPlan]): List[(LogicalPlan,
    List[Expression])] = xs match {
    case Nil =>Nil

    // special case lists of length 1 and 2 for better performance
    case t :: Nil => List((t, Nil))
    case t :: u :: Nil =>
      val conditions1 = createJoinCondition(Seq(t), Seq(u)).toList
      val conditions2 = createJoinCondition(Seq(u), Seq(t)).toList

      if (conditions1.nonEmpty && conditions2.nonEmpty) {

        List((createOrderedJoin(Seq(t, u), conditions1), conditions1))
      } else
        Nil

    case _ =>
      for ((y, ys) <- selections(xs); ps <- permute(ys); conditions <- Seq(createJoinCondition(Seq(ps._1), Seq(y))) if(conditions.nonEmpty))
        yield (createOrderedJoin(Seq(ps._1,y),conditions ), ps._2 ++ conditions)

  }



  //private[this] def CreateOptimizedReorderedPlan(plan : LogicalPlan , targetPlan : LogicalPlan): LogicalPlan = {

//
  //}

}

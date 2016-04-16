package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by alex on 21.03.16.
  */
class EquivalencesClass {

  @transient val sources: scala.collection.mutable.Set[Expression] =
    scala.collection.mutable.Set()
  @transient val derived: scala.collection.mutable.Set[Expression] =
    scala.collection.mutable.Set()

  @transient val members: scala.collection.mutable.Set[EquivalenceMember] =
    scala.collection.mutable.Set()
  @transient val relations: scala.collection.mutable.Set[LogicalPlan] =
    scala.collection.mutable.Set()

  @transient var merged: Boolean = false
  @transient var mergedLink: EquivalencesClass = null


  /**
    * Alex: Add a join condition to this EquivalenceClass.
     * */

/*  def addEquivalence(condition: Expression, children: Seq[LogicalPlan]): Unit = {


    val left = children.head
    val right = children.tail

    condition match {
      case e: EqualTo =>
        members += EquivalenceMember(Set(left), e.left)
        members += EquivalenceMember(right.toSet, e.right)


    }
    relations ++= children
    sources += condition

  }*/

  def addEquivalence(condition: Expression, children: Seq[LogicalPlan]): Unit = {

    condition match {

      case e@EqualTo(l, r) =>
        val left = children.find(plan => l.references.subsetOf(plan.outputSet))
        val right = children.find(plan => r.references.subsetOf(plan.outputSet))
        if (left.isDefined && right.isDefined) {
          members += EquivalenceMember(left.toSet, l)
          members += EquivalenceMember(right.toSet, r)
          relations ++= left.toSeq ++ right.toSeq
          sources += condition
        }

      case _ => return


    }




  }

  def generateJoinImpliedEqualities(left: Set[LogicalPlan],
                                    right: Set[LogicalPlan]): Option[Expression] = {


    val joinRelations = left.union(right)

    // find all members that can be part of this join
    val qualifiedMembers = members.filter(member => member.relations.subsetOf(joinRelations))

    val leftMember = qualifiedMembers.find(member => member.relations.subsetOf(left))
    val rightMember = qualifiedMembers.find(member => member.relations.subsetOf(right))

    // we expect rightMember be size 1 as we are onl considering left deep plans
    if (leftMember.isDefined && rightMember.isDefined) {

      val leftAttr = leftMember.get.outputSet
      val rightAttr = rightMember.get.outputSet

      // Look at the source conditions
      val sourceCond = sources.find({
        case e: EqualTo => (e.left == leftAttr) && (e.right == rightAttr)
        case _ => false

      })
      // Look at derived conditions if not source found
      if (sourceCond.isEmpty) {

        val derivedCond = derived.find({
          case e: EqualTo => (e.left == leftAttr) && (e.right == rightAttr)
          case _ => false

        })
        // Create a new condition if no condition is  found so far
        if (derivedCond.isEmpty) {

          val newCond = EqualTo(leftAttr, rightAttr).withNewChildren(Seq(leftAttr, rightAttr))
          derived += newCond
          return Some(newCond)

        }
        return derivedCond

      }
      return sourceCond

    }

    None

  }

  private[this] def mergeWith(that: EquivalencesClass): EquivalencesClass = {

    val newEquivClass = new EquivalencesClass()
    def merge(src: EquivalencesClass, dest: EquivalencesClass): Unit = {
      dest.sources ++= src.sources
      dest.members ++= src.members
      dest.relations ++= src.relations
      src.merged = true
      src.mergedLink = dest
    }

    merge(this, newEquivClass)
    merge(that, newEquivClass)
    newEquivClass

  }


}

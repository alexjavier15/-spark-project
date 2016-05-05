package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by alex on 21.03.16.
  */
class EquivalencesClass {

  @transient val conditions: scala.collection.mutable.Set[Expression] =
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

      case e@EqualTo(l : AttributeReference, r : AttributeReference) =>
        val left = children.find(plan => l.references.subsetOf(plan.outputSet))
        val right = children.find(plan => r.references.subsetOf(plan.outputSet))
        if (left.isDefined && right.isDefined) {
          members += EquivalenceMember(left.toSet, l)
          members += EquivalenceMember(right.toSet, r)
          relations ++= left.toSeq ++ right.toSeq
          conditions += condition
        }


      case _ =>


    }




  }

  def generateJoinImpliedEqualities(left: Set[LogicalPlan],
                                    right: Set[LogicalPlan]): Seq[Expression] = {


    val joinRelations = left.union(right)
    // find all members that can be part of this join
    val qualifiedMembers = members.filter(member => member.relations.subsetOf(joinRelations))

    val leftMembers = qualifiedMembers.filter(member => member.relations.subsetOf(left))
    val rightMembers = qualifiedMembers.filter(member => member.relations.subsetOf(right))

    val builtConditions = buildCondtions(leftMembers.toSeq, rightMembers.toSeq)

    conditions++=builtConditions
    builtConditions

  }




  private[this] def buildCondtions(leftMembers : Seq[EquivalenceMember],
    rightMembers :Seq[EquivalenceMember]): Seq[Expression] = {

      for (l <- leftMembers;   r <- rightMembers)
             yield
               EqualTo (l.outputSet, r.outputSet).withNewChildren(Seq(l.outputSet, r.outputSet))





    }

  def canMergeWith(that: EquivalencesClass): Boolean =  this.members.isEmpty  ||
    that.members.isEmpty ||
    (this.members & that.members).nonEmpty
  /**Merge this Equivalence class with a compatible class if possible or return this
    * Equivalence class unchange. Two equivalence classes are compatible
    * if and only if the have at least one common member.
    *
    * @param that the EquivalencesClass to emrge with
    * @return the merged EquivalencesClass, ot this  equivalence class unchanged.
    */

 def mergeWith(that: EquivalencesClass): EquivalencesClass = {


    def merge(src: EquivalencesClass, dest: EquivalencesClass): EquivalencesClass = {
      val newEquivClass = new EquivalencesClass()
      newEquivClass.conditions++= dest.conditions++src.conditions
      newEquivClass.members ++= dest.members++src.members
      newEquivClass.relations ++= dest.relations++src.relations
      newEquivClass.merged = true
      newEquivClass.mergedLink = src
      newEquivClass
    }
   if(canMergeWith(that))
     merge(this, that)
    else
      this


  }

  override def toString: String = "Conditions : "+ conditions.toString()+
  "\\nMembers: "+members.toString()
}

package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable

/**
  * Created by alex on 21.03.16.
  */
class EquivalencesClass extends Logging{

  @transient val conditions=  mutable.HashMap[Int, Expression]()
    scala.collection.mutable.Set()


  @transient val members: scala.collection.mutable.Set[EquivalenceMember] =
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

  def addEquivalence(condition: Expression): Unit = {

    condition match {

      case e@EqualTo(l : AttributeReference, r : AttributeReference) =>

          members += EquivalenceMember( l)
          members += EquivalenceMember( r)
          conditions += (condition.semanticHash() -> condition)

      case _ =>


    }




  }

  protected def canEvaluate(member: EquivalenceMember, plans: Set[LogicalPlan]): Boolean ={

    val outputset = AttributeSet(plans.flatMap(plan=>plan.outputSet))
    member.outputSet.references.subsetOf(outputset)
  }

  def generateJoinImpliedEqualities(left: Set[LogicalPlan],
                                    right: Set[LogicalPlan]): Seq[Expression] = {


    val leftMembers = members.filter(canEvaluate(_,left))
    val rightMembers = members.filter(canEvaluate(_,right))

    val builtConditions = buildCondtions(leftMembers.toSeq, rightMembers.toSeq)
    //builtConditions.filter(x => conditions.contains(x.semanticHash()))
    builtConditions.foreach(condition=>
      conditions += (condition.semanticHash() -> condition)
    )
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
      logDebug("Merging :"+this.toString + " with :" + that.toString)
      val newEquivClass = new EquivalencesClass()
      newEquivClass.conditions++= dest.conditions++src.conditions
      newEquivClass.members ++= dest.members++src.members
      newEquivClass.merged = true
      newEquivClass.mergedLink = src
      newEquivClass
    }
   if(canMergeWith(that))
     merge(this, that)
    else
      this


  }

  override def toString: String = "\n+[-Conditions : "+ conditions.toString()+
  "\n -Members: "+members.toString()+"]"
}

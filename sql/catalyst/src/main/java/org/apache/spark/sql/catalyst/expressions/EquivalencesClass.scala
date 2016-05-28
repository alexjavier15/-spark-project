package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

import scala.collection.mutable.{HashMap, HashSet}

/**
  * Created by alex on 21.03.16.
  */
class EquivalencesClass extends Logging{

  @transient val conditions=  HashMap[Int, Expression]()
    scala.collection.mutable.Set()


  @transient val members =  HashSet[EquivalenceMember] ()


  @transient var merged: Boolean = false
  @transient var mergedLink: EquivalencesClass = null


  /**
    * Alex: Add a join condition to this EquivalenceClass.
     * */


  def addEquivalence(condition: Expression): Unit = {

    condition match {

      case e@EqualTo(l : AttributeReference, r : AttributeReference) =>


          members += EquivalenceMember( l)
          members += EquivalenceMember( r)
          conditions += (condition.semanticHash() -> condition)
          condition.equivalencesClass = Some(this)

      case _ =>


    }




  }

  protected def canEvaluate(member: EquivalenceMember, plan: LogicalPlan): Boolean ={


    member.outputSet.references.subsetOf(plan.outputSet)
  }

  def generateJoinImpliedEqualities(left: LogicalPlan,
                                    right: LogicalPlan): Option[Expression] = {


    val leftMembers = members.filter(canEvaluate(_,left))
    val rightMembers = members.filter(canEvaluate(_,right))


   val condSet = for( l <-leftMembers ; r <-rightMembers)
        yield EqualTo(l.outputSet,r.outputSet).withNewChildren(Seq(l,r).map(_.outputSet))

        condSet.foreach {c =>
          c.equivalencesClass = Some(this)
          conditions += c.semanticHash() -> c
        }

    // Shuffle optimization
   left match {

      case join @ Join(_,_,_,Some(condition)) =>
        val sameKey =  condSet.find( c => c.references.intersect(condition.references).nonEmpty)
        if(sameKey.isEmpty)
          condSet.headOption
        else
          sameKey


      case _ => condSet.headOption
    }





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

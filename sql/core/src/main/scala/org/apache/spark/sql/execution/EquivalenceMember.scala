package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by alex on 21.03.16.
  */
case class EquivalenceMember(relations: Set[LogicalPlan], outputSet: Expression) {


  override def equals(o: Any): Boolean = o match {

    case o: EquivalenceMember => (relations == o.relations) && (outputSet == o.outputSet)
    case _ => false


  }

  override def toString: String = relations.map(rel=>"("+rel.nodeName +","+ outputSet.toString+")" ).mkString(",")
}

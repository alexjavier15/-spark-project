package org.apache.spark.sql.catalyst.expressions

/**
  * Created by alex on 21.03.16.
  */
case class EquivalenceMember(outputSet: Expression) {


  override def equals(o: Any): Boolean = o match {

    case o: EquivalenceMember => (outputSet == o.outputSet)
    case _ => false


  }

  override def toString: String = outputSet.map(o=>"("+o.toString+")" ).mkString(",")
}

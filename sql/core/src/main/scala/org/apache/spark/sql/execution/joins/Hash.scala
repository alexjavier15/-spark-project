package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Projection}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

/**
  * Created by alex on 28.03.16.
  */
case class Hash(child :SparkPlan,
                keyGenerator: Projection,
                sizeEstimate: Int = 64) extends  UnaryNode{




  /**
    * Overridden by concrete implementations of SparkPlan.
    * Produces the result of the query as an RDD[InternalRow]
    */
  override protected def doExecute(): RDD[InternalRow] = {


   null

  }

  override def output: Seq[Attribute] = ???


}

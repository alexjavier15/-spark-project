package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.rules.CacheHelper

import scala.collection.mutable

/**
  * Created by alex on 05.05.16.
  */
private[execution] class SparkPlanCache extends CacheHelper{


  val _sparkPlans : mutable.Map[Int,SparkPlan] =  mutable.Map[Int, SparkPlan]()


  var _sparkPlansHits = 0

  def addSparkPlan(simpleHash : Int, sparkPlan: SparkPlan): Unit = add(simpleHash,sparkPlan,_sparkPlans)

  def getSparkPlan(simpleHash : Int): Option[SparkPlan] = get(simpleHash,_sparkPlans)


  override def get[T](simpleHash: Int, map: mutable.Map[Int,T]): Option[T]={
    val res = super.get(simpleHash,map)
    if(res.isDefined)
      _sparkPlansHits+=1
    res
  }

  def clear():Unit={

  println("***Clearing PlanCache (SparkPlans) Cache. adds: "+_sparkPlans.size+", hits: "+_sparkPlansHits+"*******")
  _sparkPlans.clear()
  _sparkPlansHits=0

}


}
object SparkPlanCache{

  val cache : SparkPlanCache= new SparkPlanCache

  def add(simpleHash : Int, sparkPlan: SparkPlan): Unit = cache.addSparkPlan(simpleHash,sparkPlan)
  def get(simpleHash: Int): Option[SparkPlan] = cache.getSparkPlan(simpleHash)
  def clear(): Unit = cache.clear()

}

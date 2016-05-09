package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable

/**
  * Created by alex on 05.05.16.
  */
trait CacheHelper{
  def add[T](simpleHash: Int, plan : T , map: mutable.Map[Int,T]): Unit={
    if(!map.contains(simpleHash)){
      map+=(simpleHash-> plan)
    }

  }
  def get[T](simpleHash: Int, map: mutable.Map[Int,T]): Option[T]={
    val res =map.get(simpleHash)
    res
  }


}

private[rules] class PlanCache extends CacheHelper {


  val _anylyzedPlans : mutable.Map[Int,LogicalPlan] =  mutable.Map[Int, LogicalPlan]()
  val _optimizedPlans : mutable.Map[Int,LogicalPlan] =  mutable.Map[Int, LogicalPlan]()

  var _analyzedHits = 0
  var _optimizedHits = 0



  def addAnalyzedPlan(simpleHash : Int, logicalPlan: LogicalPlan): Unit = add(simpleHash,logicalPlan,_anylyzedPlans)

  def getAnalyzedPlan(simpleHash : Int): Option[LogicalPlan] =   get(simpleHash,_anylyzedPlans)

  def addOptimizedPlan(simpleHash : Int, logicalPlan: LogicalPlan): Unit =  add(simpleHash,logicalPlan,_optimizedPlans)

  def getOptimizedPlan(simpleHash : Int): Option[LogicalPlan] =    get(simpleHash,_optimizedPlans)

 override def get[T](simpleHash: Int, map: mutable.Map[Int,T]): Option[T]={
    val res = super.get(simpleHash,map)
    if(res.isDefined)
      map match{
        case m if m.equals(_anylyzedPlans) => _analyzedHits+=1
        case m if m.equals(_optimizedPlans) => _optimizedHits+=1
        case _ =>
      }

    res
  }


  def clear():Unit={

    println("***Clearing PlanCache (AnalyzedPlans) Cache. adds: "+_anylyzedPlans.size+", hits: "+_analyzedHits+"*******")
    println("***Clearing PlanCache (OptimzedPlans) Cache. adds: "+_optimizedPlans.size+", hits: "+_optimizedHits+"*******")

    _anylyzedPlans.clear()
    _optimizedPlans.clear()
    _analyzedHits=0
    _optimizedHits=0
  }


}
object PlanCache{

  val cache : PlanCache= new PlanCache

  def addAnalyzed(simpleHash : Int, plan: LogicalPlan): Unit = cache.addAnalyzedPlan(simpleHash,plan)
  def getAnalyzed(simpleHash: Int): Option[LogicalPlan] = cache.getAnalyzedPlan(simpleHash)
  def addOptimized(simpleHash : Int, plan: LogicalPlan): Unit = cache.addOptimizedPlan(simpleHash,plan)
  def getOptimized(simpleHash: Int): Option[LogicalPlan] = cache.getOptimizedPlan(simpleHash)
  def clear(): Unit = cache.clear()

}
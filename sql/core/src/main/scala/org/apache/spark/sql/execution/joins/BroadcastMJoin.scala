/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import java.util.concurrent.ConcurrentMap

import com.google.common.collect.MapMaker
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.pf
import org.apache.spark.sql.execution.datasources.pf.PFRelation.{CHUNK_NUM, CHUNK_RECORDS}
import org.apache.spark.sql.execution.{DataSourceScan, Filter, SparkPlan, UnaryNode}
import org.apache.spark.sql.sources.{HadoopFsRelation, PFileCatalog}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class BroadcastMJoin(
            child : SparkPlan,
            baseRelations : Seq[SparkPlan],
            subplans : Option[Seq[SparkPlan]]
                         )
  extends UnaryNode {

  private val chunkedSubplans : Seq[Seq[Int]] = makeBucktedSubplans
  private var currentSubplans : Seq[Seq[Int]] = Seq(Seq())
  var key = -1
  // Keeps track of all persisted RDDs
  private[this] val persistentHashTables = {
    val map: ConcurrentMap[Int, broadcast.Broadcast[HashedRelation]] = new MapMaker().
      weakValues().makeMap[Int, broadcast.Broadcast[HashedRelation]]()
    map.asScala
  }

  private [this]  def initializeHashTables():Unit = {
    baseRelations.
      map(baseRel => {
        val idx = baseRelations.indexOf(baseRel)
        val hashRelation = executeBroadcast[HashedRelation]
        persistHashTable(idx, hashRelation)

      })
 }

  private[this] def persistHashTable (idx : Int, hashRelation :  broadcast.Broadcast[HashedRelation]){
    persistentHashTables(idx) =hashRelation

  }

  private[this] def makeBucktedSubplans(): Seq[Seq[Int]] = {

  val metadataList = baseRelations.map( relation => relation match {
    case Filter(_, d @ DataSourceScan(_,rdd,_,metadata)) =>
      key = rdd.id
      metadata.get(CHUNK_NUM).getOrElse("1")
    case _ => "1"
  }
    )
    assert( !metadataList.contains("") )
    val numChunksPerRel = metadataList.map(_.toInt)

    def permuteWithoutOrdering ( seq : Seq[Int]):Seq[Seq[Int]] ={
      seq match {

        case x::Nil => Seq.iterate(Seq(1),seq.head)(x=> Seq(x.head+1))
        case _ =>
          for ( y<- Seq.iterate(1,seq.head)(y=> y+1); x <-permuteWithoutOrdering(seq.tail))
            yield(y+: x)
      }
    }

   val res = permuteWithoutOrdering(numChunksPerRel)

    println(res)
    res
  }

 // def  streamed(start : Int ,iterator : Iterator[Int]) : Stream[(Int, Int)] = (start ,iterator.next())#::streamed(start+1,iterator)
def getSubplansForChunk(relIdx : Int, chunkIdx : Int) : Seq[Seq[Int]] =
{
  assert(relIdx <= baseRelations.length)
  val qualified = chunkedSubplans.filter( subplan => subplan(relIdx).equals(chunkIdx))
    qualified
}
private def chunktoDrop() : (Int,Int) = {

  val allSubplans = currentSubplans
  //++newSubplans

  val mapped = allSubplans.flatMap(subplan =>{
    val iterator = subplan.toIterator
    Seq.iterate((1,iterator.next), subplan.length)(x => (x._1+1 ,iterator.next))

  })

  val groupedChunks = mapped.groupBy(x => x)
  val numSuplansByChunk = groupedChunks.map( chunk => (chunk._1 ,chunk._2.length) )

  numSuplansByChunk.minBy(_._2)._1

  // if there's other criteria jsut add here


}
private def chunkHasSubplans(relIdx : Int, chunkIdx : Int) : Boolean = {

  getSubplansForChunk(relIdx,chunkIdx).nonEmpty

}

 private def getNextChunk(relIdx : Int ) : Int ={



   val relation = baseRelations(relIdx)

   relation  match {
 //    case DataSourceScan(_, _, fp :  pf.DefaultSource, metadata) =>  fp.getNextChunkID
     case _ => -1
   }




 }


/*  private [this] def getNumOfBuckects(baseRelation : SparkPlan) : Int = {

    baseRelation match {

      case h : PFRelation =>

    }



  }*/
  override protected def doExecute(): RDD[InternalRow] = {
  if(key != -1){
    sqlContext.sparkContext.setLocalProperty("chunksFor"+key,"1")
    println("setting chunks for : " + key)
  }
  child.execute()
}

  override def output: Seq[Attribute] = child.output


}

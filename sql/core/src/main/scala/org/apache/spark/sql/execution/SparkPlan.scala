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

package org.apache.spark.sql.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.apache.spark.{SparkEnv, broadcast}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetric}
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ThreadUtils

import scala.reflect.ClassTag

/**
 * The base class for physical operators.
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  val OUTPUT_ROWS_KEY : String = "numOutputRows"
  val NUM_PARTITIONS_KEY : String = "numPartitions"
  //val NUM_ROWS_KEY : String = "numRows"
  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  protected[spark] final val sqlContext = SQLContext.getActive().orNull

  protected def sparkContext = sqlContext.sparkContext
  var statistics : Option[Map[String,Long]] = None
  var hasSelectivity : Boolean = false

  def selectivity() : Double = {

    if(getOutputRows <= 0)
     Double.MinPositiveValue
    else
      children.map(x => 1.0/x.getOutputRows).foldLeft(getOutputRows.toDouble)(_*_)

  }
  protected def  capRows(rows : Long): Long = rows match {
    case r if r <= 0 => 1L
    case r => r
  }


  def getNumPartitions: Long = {
    if (metrics.contains(NUM_PARTITIONS_KEY)) {
      val num = metrics(NUM_PARTITIONS_KEY).asInstanceOf[LongSQLMetric].value.value
      num match {
        case n: Long if n <= 0L => 2
        case _ => num
      }
    }
    else
      2 //min parallelism
  }
  def getOutputRows: Long = {
    if (metrics.contains(OUTPUT_ROWS_KEY)) {
      val rows = metrics(OUTPUT_ROWS_KEY).asInstanceOf[LongSQLMetric].value.value
      capRows(rows)
    }else
      throw new UnsupportedOperationException(s"$nodeName does not contains output rows metric")

  }

  /*def rows() : Long ={
    if (metrics.contains(NUM_ROWS_KEY)) {
    val rows = metrics(NUM_ROWS_KEY).asInstanceOf[LongSQLMetric].value.value
    capRows(rows)
  }else
  throw new UnsupportedOperationException(s"$nodeName does not contains num rows metric")
}*/



def planCost() : Long = {
    throw new UnsupportedOperationException(s"$nodeName does not implement plan costing")
  }

  def extractNodes[T: ClassTag] : Seq[T] = {

    this match {
      case node : T => Seq(node) ++ children.flatMap(_.extractNodes)
      case  _ => children.flatMap(_.extractNodes)

    }

  }

 def simpleHash : Int= {
   throw new UnsupportedOperationException(s"$nodeName does not implement simpleHash")
 }
 def semanticHash : Int= {
    throw new UnsupportedOperationException(s"$nodeName does not implement semanticHash")
  }

  def numLeaves : Long = {
    throw new UnsupportedOperationException(s"$nodeName does not implement numLeaves")
  }
  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of subexpressionEliminationEnabled will be set by the deserializer after the
  // constructor has run.
  val subexpressionEliminationEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.subexpressionEliminationEnabled
  } else {
    false
  }

  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /** Overridden make copy also propagates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    SQLContext.setActive(sqlContext)
    super.makeCopy(newArgs)
  }

  /**
   * Return all metadata that describes more details of this SparkPlan.
   */
  private[sql] def metadata: Map[String, String] = Map.empty

  /**
   * Return all metrics containing metrics of this SparkPlan.
   */
  private[sql] def metrics: Map[String, SQLMetric[_, _]] = Map.empty

  /**
    * Reset all the metrics.
    */
  private[sql] def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
  }


  /**
   * Return a LongSQLMetric according to the name.
   */
  private[sql] def longMetric(name: String): LongSQLMetric =
    metrics(name).asInstanceOf[LongSQLMetric]


  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute after
   * preparations. Concrete implementations of SparkPlan should override doExecute.
   */
  final def execute(): RDD[InternalRow] = executeQuery {
    val rdd = doExecute()
    if (metrics.contains(NUM_PARTITIONS_KEY)) {
      val numPartitions = longMetric(NUM_PARTITIONS_KEY)
      numPartitions.reset()
      numPartitions += rdd.getNumPartitions
    }
    rdd
  }

  /**
   * Returns the result of this query as a broadcast variable by delegating to doBroadcast after
   * preparations. Concrete implementations of SparkPlan should override doBroadcast.
   */
  final def executeBroadcast[T](): broadcast.Broadcast[T] = executeQuery {
    doExecuteBroadcast()
  }

  /**
   * Execute a query after preparing the query and adding query plan information to created RDDs
   * for visualization.
   */
  private final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      waitForSubqueries()
      query
    }
  }

  /**
   * List of (uncorrelated scalar subquery, future holding the subquery result) for this plan node.
   * This list is populated by [[prepareSubqueries]], which is called in [[prepare]].
   */
  @transient
  private val subqueryResults = new ArrayBuffer[(ScalarSubquery, Future[Array[InternalRow]])]

  /**
   * Finds scalar subquery expressions in this plan node and starts evaluating them.
   * The list of subqueries are added to [[subqueryResults]].
   */
  protected def prepareSubqueries(): Unit = {
    val allSubqueries = expressions.flatMap(_.collect {case e: ScalarSubquery => e})
    allSubqueries.asInstanceOf[Seq[ScalarSubquery]].foreach { e =>
      val futureResult = Future {
        // Each subquery should return only one row (and one column). We take two here and throws
        // an exception later if the number of rows is greater than one.
        e.executedPlan.executeTake(2)
      }(SparkPlan.subqueryExecutionContext)
      subqueryResults += e -> futureResult
    }
  }

  /**
   * Blocks the thread until all subqueries finish evaluation and update the results.
   */
  protected def waitForSubqueries(): Unit = {
    // fill in the result of subqueries
    subqueryResults.foreach { case (e, futureResult) =>
      val rows = Await.result(futureResult, Duration.Inf)
      if (rows.length > 1) {
        sys.error(s"more than one row returned by a subquery used as an expression:\n${e.plan}")
      }
      if (rows.length == 1) {
        assert(rows(0).numFields == 1,
          s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
        e.updateResult(rows(0).get(0, e.dataType))
      } else {
        // If there is no rows returned, the result should be null.
        e.updateResult(null)
      }
    }
    subqueryResults.clear()
  }

  override def equals(o: scala.Any): Boolean =  o match {
      case s : SparkPlan => this.sameResult(s)
      case _  => false

    }




  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare()
      prepareSubqueries()
      children.foreach(_.prepare())
    }

  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as a broadcast variable.
   */
  protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    throw new UnsupportedOperationException(s"$nodeName does not implement doExecuteBroadcast")
  }

  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(n: Int = -1): RDD[Array[Byte]] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      while (iter.hasNext && (n < 0 || count < n)) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator(bos.toByteArray)
    }
  }

  /**
   * Decode the byte arrays back to UnsafeRows and put them into buffer.
   */
  private def decodeUnsafeRows(bytes: Array[Byte], buffer: ArrayBuffer[InternalRow]): Unit = {
    val nFields = schema.length

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(codec.compressedInputStream(bis))
    var sizeOfNextRow = ins.readInt()
    while (sizeOfNextRow >= 0) {
      val bs = new Array[Byte](sizeOfNextRow)
      ins.readFully(bs)
      val row = new UnsafeRow(nFields)
      row.pointTo(bs, sizeOfNextRow)
      buffer += row
      sizeOfNextRow = ins.readInt()
    }
  }

  def printMetrics : Unit = {
    logInfo(nodeName + " :" +metrics)
    logInfo("Selectivity" + " :" +selectivity())
    children.foreach(_.printMetrics)

  }
  def resetChildrenMetrics : Unit = {
    logInfo("Reseting metric for :"+nodeName )
    resetMetrics()
    children.foreach(_.resetChildrenMetrics)

  }

  def executeCollectSample(num : Int): Array[InternalRow] = executeCollect()
  /**
   * Runs this query returning the result as an array.
   */

  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { bytes =>
      decodeUnsafeRows(bytes, results)
    }

    results.toArray

  }

  @transient
  private lazy val metricsFuture: Future[broadcast.Broadcast[Map[String, SQLMetric[_, _]]]] = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // Note that we use .executeCollect() because we don't want to convert data to Scala types
        val input: Array[InternalRow] = executeCollect()
        logDebug("metrics " + metrics.get("numOutputRows"))
        // Construct and broadcast the relation.
        sparkContext.broadcast[Map[String, SQLMetric[_, _]]](metrics)
      }
    }(ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange", 128)))
  }

  def executeBroadcastMetrics(): broadcast.Broadcast[Map[String, SQLMetric[_, _]]] ={

    val result = Await.result(metricsFuture, Duration.Inf)
    result.asInstanceOf[broadcast.Broadcast[Map[String, SQLMetric[_, _]]]]

  }


  /**
   * Runs this query returning the result as an array, using external Row format.
   */
  def executeCollectPublic(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    executeCollect().map(converter(_).asInstanceOf[Row])
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = getByteArrayRdd(n)

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size
      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Array[Byte]]) => if (it.hasNext) it.next() else Array.empty, p)

      res.foreach { r =>
        decodeUnsafeRows(r.asInstanceOf[Array[Byte]], buf)
      }

      partsScanned += p.size
    }
    if (buf.size > n) {


      buf.take(n).toArray
    } else {

      buf.toArray

    }
  }

  private[this] def isTesting: Boolean = sys.props.contains("spark.testing")

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean = false): () => MutableProjection = {
    log.debug(s"Creating MutableProj: $expressions, inputSchema: $inputSchema")
    GenerateMutableProjection.generate(expressions, inputSchema, useSubexprElimination)
  }

  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (InternalRow) => Boolean = {
    GeneratePredicate.generate(expression, inputSchema)
  }

  protected def newOrdering(
      order: Seq[SortOrder], inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    GenerateOrdering.generate(order, inputSchema)
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

object SparkPlan {
  private[execution] val subqueryExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery", 16))
}

private[sql] trait LeafNode extends SparkPlan {
  override def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
  override def planCost(): Long =  getOutputRows
  override def numLeaves: Long = 1L
  override def semanticHash: Int =  this.simpleHash
  override def selectivity() : Double = 1.0
}

private[sql] trait UnaryNode extends SparkPlan {
  def child: SparkPlan

  override private[sql] lazy val metrics =  child.metrics
  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def planCost: Long =  child.planCost()

  override def numLeaves: Long = child.numLeaves

  override def  simpleHash: Int =   child.simpleHash


  //override def rows(): Long = child.rows()

  override def semanticHash: Int =  child.semanticHash

}

private[sql] trait BinaryNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override def children: Seq[SparkPlan] = Seq(left, right)

  override def planCost(): Long =  left.planCost() + left.getOutputRows

  override def numLeaves: Long = left.numLeaves + right.numLeaves

  override def simpleHash : Int ={

    var h = 17
    h = h * 37 + left.simpleHash
    h = h * 37 + right.simpleHash
    h

  }
  override def semanticHash: Int = {
    var h = left.semanticHash + right.semanticHash
    h

  }


}

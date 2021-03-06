package org.apache.spark.sql.execution.datasources.pf

import java.io.{File, PrintStream}
import java.net.{InetAddress, Socket}
import java.nio.file.{FileSystem, FileSystems}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.sources.{Filter, HadoopFsRelation, PFileCatalog}
import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.BufferedSource
import scala.io.Source._

/**
  * Created by alex on 18.03.16.
  */

case class CachedRDD(rdd: RDD[InternalRow] ,relation : HadoopPfRelation, requiredColumns: Array[String] , filters: Array[Filter]){

  def sameResult (relation0 : HadoopPfRelation,
                   requiredColumns0: Array[String] ,
                   filters0: Array[Filter]) : Boolean ={

    if(relation.simpleHash == relation0.simpleHash){
      !requiredColumns0.exists( s => !requiredColumns.contains(s)) &&
      !filters0.exists(f => !filters.contains(f))

    }else
      false

  }

}
class HadoopPfRelation(override val sqlContext: SQLContext,
                       @transient pFLocation: PFileCatalog,
                            partitionSchema: StructType,
                            dataSchema: StructType,
                            bucketSpec: Option[BucketSpec],
                       override val fileFormat: DefaultSource,
                            options: Map[String, String],
                            val pFileDesc : PFileDesc,
                            parent : HadoopPfRelation = null) extends
  HadoopFsRelation(sqlContext,
    pFLocation,
    partitionSchema,
    dataSchema,
    bucketSpec,
    fileFormat,
    options) with Serializable{

  var uniqueID : Int = pFileDesc.hashCode()

  def hasParent : Boolean = parent!=null

   /* println("Initiating HaddopPfRelation  ")
    pFLocation.allFiles().foreach(f=>{
      import sys.process._
      "xattr -l "+ f.getPath.toString.substring(5) !
      })*/


  def isChild(relation : HadoopPfRelation): Boolean = hasParent && parent == relation
  def  splitHadoopPfRelation(): Seq[HadoopPfRelation] = {

     pFLocation.splitPFileCatalog().map(fc =>
       new HadoopPfRelation(sqlContext ,fc,partitionSchema,dataSchema,bucketSpec,fileFormat,options,pFileDesc, this))

  }


  override def simpleHash: Int = {
    var h = 17
    h = 37 * location.hashCode() + pFileDesc.hashCode()
    h
  }

  override def semanticHash: Int = pFileDesc.hashCode()

  override def equals(o: scala.Any): Boolean = {
    o match{
    case h : HadoopPfRelation if  pFileDesc.equals(h.pFileDesc) &&
      pFLocation.equals(h.location) => true
    case _ => false

  }}

  override val schema: StructType = {
    if (hasParent) {
      parent.schema
    } else dataSchema
  }

  /*val fileDesc = PFRelation.readPFileInfo(location.paths.head.getName)


    override  val paths = fileDesc.chunkPFArray.map(chunk  => "file:"+chunk.data_location)

    override  lazy val dataSchema: StructType = maybeDataSchema match {
      case Some(structType) => structType
      case None => fileDesc.strucType
    }
  */
  override def toString: String = { super.toString + ", " + pFLocation.toString }
}

protected[sql] object PFRelation extends Logging {

  lazy val sparkContext = SparkContext.getOrCreate()
  private var _dataSources = mutable.Set[HadoopPfRelation]()
  private val rddCache = mutable.HashMap[Int , ArrayBuffer[CachedRDD]]()
  val CHUNK_NUM = "ChunkNumber"
  val CHUNK_RECORDS = "ChunkRecords"
  val GET_CHUNK = "GET:"
  val DROP_CHUNK = "DROP:"
  val NEXT_CHUNK = "NEXT:"
  val REMOTE_PORT = 9999
  val REMOTE_ADRESS = "localhost"
  var hits : Int  =0

  var connector = new Socket()

  def executeCommand(command: String): Array[String] = {


    val in = getInputChannel()
    val out = getOutputChannel()

    out.println(command)
    out.flush

    val reply = in.getLines().toArray
    reply.foreach(x => println("Read :" + x))

    out.println("END:NOW")

    out.flush
    connector.close
    reply

  }

  def getCachedRDD(relation : HadoopPfRelation, requiredColumns: Array[String] , filters: Array[Filter]): Option[RDD[InternalRow]] ={

    val buffer = rddCache.getOrElseUpdate(relation.simpleHash, ArrayBuffer[CachedRDD]())
    val sameRdd = buffer.find( rdd => rdd.sameResult(relation,requiredColumns,filters))
    if(sameRdd.isDefined){
      hits+=1
      Some(sameRdd.get.rdd)
    }
    else
      None

  }

  def cacheRDD(relation : HadoopPfRelation, cachedRDD : CachedRDD) ={

    val buffer = rddCache.getOrElseUpdate(relation.simpleHash, ArrayBuffer[CachedRDD]())
      buffer+=cachedRDD

  }

  def getNextChunkID(): Int = {
    val out = getOutputChannel()
    out.println(NEXT_CHUNK)
    val in = getInputChannel()
    in.getLines().next().toInt

  }

  private def getInputChannel(): BufferedSource = {

    if (needToReconnect) {
      connectToPelicanServer()
      println("Need To Reconnect  IN!")
    }

    new BufferedSource(connector.getInputStream())


  }

  def needToReconnect = connector.isClosed || !connector.isConnected

  // Execute a command and return the reply

  def connectToPelicanServer(): Unit = {

    if (!connector.isClosed)
      connector.close

    connector = new Socket(InetAddress.getByName(REMOTE_ADRESS), REMOTE_PORT)


  }

  private def getOutputChannel(): PrintStream = {
    if (needToReconnect) {
      println("Need To Reconnect OUT!")
      connectToPelicanServer()
    }

    new PrintStream(connector.getOutputStream())

  }

  def readPFileInfo(path: String): PFileDesc = {
    implicit val formats = DefaultFormats


    extractPFMetadata[PFileDesc](path)


  }

  def extractPFMetadata[A: Manifest](path: String): A = {

    implicit val formats = DefaultFormats
    val json = fromFile(path).getLines().reduce(_ + _)
    val data = parse(json)


    val extractedClass = data.extract[A]
    extractedClass


  }


}


case class ChunkLocation(id: String,
                         holder_location: String) {
  override def toString: String = "( id: " + id + ", holder_location: " + holder_location + " )"
}


case class ChunkDesc(parent_file: String,
                     chunk_id: String,
                     chunk_size: Int,
                     num_records: Int,
                     data_location: String) {

  //private[this] readChuk(index : Int)

  override def toString: String = "( parent_file: " + parent_file + System.lineSeparator +
    "chunk_id: " + chunk_size + System.lineSeparator +
    "chunk_size: " + chunk_id + System.lineSeparator +
    "num_records: " + num_records + System.lineSeparator +
    "data_location: " + data_location + System.lineSeparator + ")"
}

case class PFileDesc(file_name: String,
                     file_size: Int,
                     num_records: Int,
                     chunk_locations: Array[ChunkLocation],
                     schema_location: String) {
  lazy val chunkPFArray = chunk_locations.map(chunk => {
    val chunkDesc = PFRelation.extractPFMetadata[ChunkDesc](chunk.holder_location)

    chunkDesc
  })
  implicit val formats = new Serializable {DefaultFormats}
  val structType: Option[StructType] = DataType.fromJson(fromFile(schema_location).getLines.reduce(_ + _)) match {
    case e: StructType => Some(e)
    case _ => None


  }

  val paths: Seq[Path] = {

    val paths0 = chunkPFArray.map(chunk => chunk.data_location)
    paths0.flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(SparkContext.getOrCreate().hadoopConfiguration)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      SparkHadoopUtil.get.globPathIfNecessary(qualified)
    }

  }


  def getChunksNum: Int = chunkPFArray.length

  // def getChunk( id: Int): Int = _


  override def toString: String = "( file_name: " + file_name + System.lineSeparator +
    "file_size: " + file_size + System.lineSeparator +
    "num_records: " + num_records + System.lineSeparator +
    "chunks: [" + System.lineSeparator +
    chunk_locations.map(v => v.toString + System.lineSeparator).reduce(_ + _) + " ])"

}


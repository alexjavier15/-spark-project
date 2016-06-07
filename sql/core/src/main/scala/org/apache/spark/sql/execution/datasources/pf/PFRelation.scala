package org.apache.spark.sql.execution.datasources.pf

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.sources.{HadoopFsRelation, PFileCatalog}
import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.io.Source._

/**
  * Created by alex on 18.03.16.
  */

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
    println("Initiating HaddopPfRelation  ")
    pFLocation.allFiles().foreach(f=>{
      import sys.process._
      "xattr -l "+ f.getPath.toString.substring(5) !
      })


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
  val CHUNK_NUM = "ChunkNumber"
  val CHUNK_RECORDS = "ChunkRecords"
  val GET_CHUNK = "GET:"
  val DROP_CHUNK = "DROP:"
  val NEXT_CHUNK = "NEXT:"
  val REMOTE_PORT = 9999
  val REMOTE_ADRESS = "localhost"








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

  val structType: Option[StructType] =readStruct()


  private def readStruct(): Option[StructType]={
    implicit val formats = new Serializable {DefaultFormats}

    val source =fromFile(schema_location)

    val struct =DataType.fromJson(source.getLines.reduce(_ + _)) match {
      case e: StructType => Some(e)
      case _ => None
    }
    source.close()
    struct

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


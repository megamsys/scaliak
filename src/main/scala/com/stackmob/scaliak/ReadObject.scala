package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.cap.VClock
import com.basho.riak.client.builders.RiakObjectBuilder
import com.basho.riak.client.http.util.{Constants => RiakConstants}
import com.basho.riak.client.{RiakLink, IRiakObject}
import com.basho.riak.client.query.indexes.{RiakIndexes, IntIndex, BinIndex}

/**
 * Represents data read from Riak
 */
case class ReadObject(key: String,
                         bucket: String,
                         contentType: String,
                         vClock: VClock,
                         bytes: Array[Byte],
                         vTag: String = "",
                         links: Option[NonEmptyList[ScaliakLink]] = none,
                         metadata: Map[String, String] = Map(),
                         lastModified: java.util.Date = new java.util.Date(System.currentTimeMillis),
                         binIndexes: Map[BinIndex, Set[String]] = Map(),
                         intIndexes: Map[IntIndex, Set[Int]] = Map()) {
  
  def vClockString = vClock.asString

  // TODO: probably should move, leaving for now since its used in a bunch of places
  def getBytes = bytes

  def stringValue = new String(bytes)

  def hasLinks = links.isDefined

  def numLinks = (links map { _.count }) | 0

  def containsLink(link: ScaliakLink) = (links map { _.list.contains(link) }) | false

  def addLink(link: ScaliakLink): ReadObject = copy(links = (link :: ~(links map { _.list })).toNel)
  
  def addLink(bucket: String, key: String, tag: String): ReadObject = addLink(ScaliakLink(bucket, key, tag))
  
  def addLinks(ls: Seq[ScaliakLink]) = copy(links = (ls ++ ~(links map { _.list })).toList.toNel)
  
  def removeLink(link: ScaliakLink): ReadObject = copy(links = (~(links map { _.list filter { _ === link } })).toNel)

  def removeLink(bucket: String, key: String, tag: String): ReadObject = removeLink(ScaliakLink(bucket, key, tag))

  def removeLinks(tag: String) = copy(links = (~(links map { _.list filterNot { _.tag === tag } })).toNel)

  def hasMetadata = !metadata.isEmpty

  def containsMetadata(key: String) = metadata.contains(key)

  def getMetadata(key: String) = metadata.get(key)

  def addMetadata(key: String, value: String): ReadObject = copy(metadata = metadata + (key -> value))

  def addMetadata(kv: (String, String)): ReadObject = addMetadata(kv._1, kv._2)

  def mergeMetadata(newMeta: Map[String, String]) = copy(metadata = metadata ++ newMeta)

  def removeMetadata(key: String) = copy(metadata = metadata - key)

  def binIndex(name: String): Option[Set[String]] = binIndexes.get(BinIndex.named(name))

  def intIndex(name: String): Option[Set[Int]] = intIndexes.get(IntIndex.named(name))

}

object ReadObject {
  import scala.collection.JavaConverters._
  implicit def IRiakObjectToScaliakObject(obj: IRiakObject): ReadObject = {
    ReadObject(
      key = obj.getKey,
      bytes = obj.getValue,
      bucket = obj.getBucket,
      vClock = obj.getVClock,
      vTag = ~(Option(obj.getVtag)),
      contentType = obj.getContentType,
      lastModified = obj.getLastModified,
      links = (obj.getLinks.asScala map { l => l: ScaliakLink }).toList.toNel,
      metadata = obj.getMeta.asScala.toMap,
      binIndexes = obj.allBinIndexes.asScala.mapValues(_.asScala.toSet).toMap,
      intIndexes = obj.allIntIndexes.asScala.mapValues(_.asScala.map(_.intValue()).toSet).toMap
    )
  }
  
}

/**
 * Represents data that is intented to be written to riak
 */
// TODO: try to get rid of underscores
sealed trait WriteObject {
  import scala.collection.JavaConverters._

  def _key: String
  def _bytes: Array[Byte]
  def _contentType: Option[String]
  def _links: Option[NonEmptyList[ScaliakLink]]
  def _metadata: Map[String, String]
  def _binIndexes: Map[BinIndex, Set[String]]
  def _intIndexes: Map[IntIndex, Set[Int]]
  def _vClock: Option[VClock]
  def _vTag: String
  def _lastModified: java.util.Date

  def asRiak(bucket: String, vClock: VClock): IRiakObject = {
    val builder = (RiakObjectBuilder.newBuilder(bucket, _key)
      withContentType (_contentType | RiakConstants.CTYPE_TEXT_UTF8)
      withValue _bytes
      withLinks ((_links map { _.list map { l => new RiakLink(l.bucket, l.key, l.tag) }}) | Nil).asJavaCollection
      withUsermeta _metadata.asJava
      withIndexes buildIndexes
    )
    if (vClock != null) builder withVClock vClock
    else _vClock foreach builder.withVClock

    if (!_vTag.isEmpty) builder.withVtag(_vTag)
    if (_lastModified != null) builder.withLastModified(_lastModified.getTime)

    builder.build
  }

  private def buildIndexes: RiakIndexes = {
    val binIndexes: java.util.Map[BinIndex, java.util.Set[String]] = new java.util.HashMap[BinIndex,java.util.Set[String]]()
    val intIndexes: java.util.Map[IntIndex, java.util.Set[java.lang.Integer]] = new java.util.HashMap[IntIndex,java.util.Set[java.lang.Integer]]()
    for { (k,v) <- _binIndexes } {
      val set: java.util.Set[String] = new java.util.HashSet[String]()
      v.foreach(set.add(_))
      binIndexes.put(k,set)
    }
    for { (k,v) <- _intIndexes } {
      val set: java.util.Set[java.lang.Integer] = new java.util.HashSet[java.lang.Integer]()
      v.foreach(set.add(_))
      intIndexes.put(k,set)
    }

    new RiakIndexes(binIndexes, intIndexes)
  }
}

object WriteObject {
  import com.basho.riak.client.http.util.{Constants => RiakConstants}
  def apply(key: String, 
            value: Array[Byte], 
            contentType: String = RiakConstants.CTYPE_TEXT_UTF8, 
            links: Option[NonEmptyList[ScaliakLink]] = none,
            metadata: Map[String, String] = Map(),
            vClock: Option[VClock] = none,
            vTag: String = "",
            binIndexes: Map[BinIndex, Set[String]] = Map(),
            intIndexes: Map[IntIndex, Set[Int]] = Map(),
            lastModified: java.util.Date = null) = new WriteObject {
    def _key = key
    def _bytes = value
    def _contentType = Option(contentType)
    def _links = links
    def _metadata = metadata
    def _vClock = vClock
    def _vTag = vTag
    def _binIndexes = binIndexes
    def _intIndexes = intIndexes
    def _lastModified = lastModified
    
  }
  
}

case class ScaliakLink(bucket: String, key: String, tag: String)
object ScaliakLink {
  implicit def riakLinkToScaliakLink(link: RiakLink): ScaliakLink = ScaliakLink(link.getBucket, link.getKey, link.getTag)
  
  implicit def ScaliakLinkEqual: Equal[ScaliakLink] = Equal.equalA
}
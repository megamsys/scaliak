/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.client.api.cap.VClock
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.util.{ Constants => RiakConstants }
import com.basho.riak.client.core.query.links.{ RiakLink }
import com.basho.riak.client.core.query.indexes._
import java.nio.charset.Charset
import scala.util.control.Exception._

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
  binIndexes: Map[String, Set[String]] = Map(),
  intIndexes: Map[String, Set[Long]] = Map()) {

  def vClockString = new String(vClock.getBytes)

  // TODO: probably should move, leaving for now since its used in a bunch of places
  def getBytes = bytes

  def stringValue = new String(bytes, getCharset)

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

  def binIndex(name: String): Option[Set[String]] = binIndexes.get(name)

  def intIndex(name: String): Option[Set[Long]] = intIndexes.get(name)

  private lazy val charsetMatcher = """.*;\s*charset=([^\(\)<>@,;:\\"/\[\]\?={}\s\t]+);?\s*.*$""".r

  private def getCharset: Charset = (contentType match {
    case charsetMatcher(charset) => catching(classOf[Exception]).opt(Charset.forName(charset))
    case _                       => None
  }).getOrElse(Charset.forName("ISO-8859-1"))

}

object ReadObject {
  import scala.collection.JavaConverters._
  implicit def IRiakObjectToScaliakObject(t: RichRiakObject): ReadObject = {
    val obj = t.obj
    ReadObject(
      key = t.key,
      bucket = t.bucket,
      bytes = obj.getValue.getValue,
      vClock = obj.getVClock,
      vTag = ~(Option(obj.getVTag)),
      contentType = obj.getContentType,
      lastModified = new java.util.Date(obj.getLastModified),
      links = (obj.getLinks.asScala map { l => l: ScaliakLink }).toList.toNel,
      metadata = obj.getUserMeta.getUserMetadata.asScala.map(m => m.getKey.toString -> m.getValue.toString).toMap,
      binIndexes = obj.getIndexes().asScala.filter(_.getType == IndexType.BIN).map(f => f.getName -> f.rawValues.asScala.toSet.map({ bv: BinaryValue => bv.toString })).toMap,
      intIndexes = obj.getIndexes().asScala.filter(_.getType == IndexType.INT).map(f => f.getName -> f.rawValues.asScala.toSet.map({ bv: BinaryValue => bv.toString.toLong })).toMap)
  }
  

}

/**
 * Represents data that is intented to be written to riak
 */
sealed trait WriteObject {
  import scala.collection.JavaConverters._

  def _key: String
  def _bytes: Array[Byte]
  def _contentType: Option[String]
  def _links: Option[NonEmptyList[ScaliakLink]]
  def _metadata: Map[String, String]
  def _binIndexes: Map[String, Set[String]]
  def _intIndexes: Map[String, Set[Long]]
  def _vClock: Option[VClock]
  def _vTag: String
  def _lastModified: java.util.Date

  def asRiak(bucket: String, vClock: VClock): RiakObject = {
    val builder = (new RiakObject().setContentType(_contentType | RiakConstants.CTYPE_TEXT_UTF8)).setValue(BinaryValue.create(_bytes))

    builder.getLinks().addLinks(
      ((_links map { _.list map { l => new RiakLink(l.bucket, l.key, l.tag) } }) | Nil).asJavaCollection)

    builder.getUserMeta().put(_metadata.asJava)
    buildIndexes(builder.getIndexes)

    if (vClock != null) builder.setVClock(vClock)
    else _vClock foreach builder.setVClock

    if (!_vTag.isEmpty) builder.setVTag(_vTag)
    if (_lastModified != null) builder.setLastModified(_lastModified.getTime)
    builder
  }

  private def buildIndexes(riakIndexes: RiakIndexes) = {
    for { (k, v) <- _binIndexes } {
      val binIndex: StringBinIndex = riakIndexes.getIndex[StringBinIndex, StringBinIndex.Name](StringBinIndex.named(k, getCharset))
      binIndex.add(v.asJavaCollection)
    }

    for { (k, v) <- _intIndexes } {
      val longIndex: LongIntIndex = riakIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named(k))
      v.map(longIndex.add(_))
    }
  }

  private lazy val charsetMatcher = """.*;\s*charset=([^\(\)<>@,;:\\"/\[\]\?={}\s\t]+);?\s*.*$""".r

  private def getCharset: Charset = (_contentType.getOrElse(new String()) match {
    case charsetMatcher(charset) => catching(classOf[Exception]).opt(Charset.forName(charset))
    case _                       => None
  }).getOrElse(Charset.forName("ISO-8859-1"))
}

object WriteObject {
  def apply(
    key: String,
    value: Array[Byte],
    contentType: String = RiakConstants.CTYPE_TEXT_UTF8,
    links: Option[NonEmptyList[ScaliakLink]] = none,
    metadata: Map[String, String] = Map(),
    vClock: Option[VClock] = none,
    vTag: String = "",
    binIndexes: Map[String, Set[String]] = Map(),
    intIndexes: Map[String, Set[Long]] = Map(),
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

case class RichRiakObject(key: String, bucket: String, obj: RiakObject)
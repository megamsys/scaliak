package com.stackmob.scaliak
package domainwrapper

import scalaz._
import Scalaz._

// not necessary unless you want to take advantage of IO monad

import com.basho.riak.client.convert._

import com.basho.riak.client.RiakLink
import com.basho.riak.client.query.indexes.{RiakIndexes, IntIndex, BinIndex}
import com.stackmob.scaliak.mapreduce._
import com.stackmob.scaliak.mapreduce.MapReduceFunctions._

import scala.collection.JavaConverters._

import com.codahale.jerkson.Json

abstract class DomainObject {
  def key: String
}

//case class DomainObject(val key: String, val value: String)
abstract class DomainObjectWrapper[T <: DomainObject](val clazz: Class[T],
                                                      val proposedBucketName: Option[String] = None,
                                                      val clientPool: ScaliakPbClientPool)(implicit mot: Manifest[T]) {

  val usermetaConverter: UsermetaConverter[T] = new UsermetaConverter[T]
  val riakIndexConverter: RiakIndexConverter[T] = new RiakIndexConverter[T]
  val riakLinksConverter: RiakLinksConverter[T] = new RiakLinksConverter[T]

  def fromJson(json: String) = {
    Json.parse[T](json)
  }

  implicit val domainConverter: ScaliakConverter[T] = ScaliakConverter.newConverter[T](
    (o: ReadObject) ⇒ {
      val domObject = Json.parse[T](o.stringValue)
      usermetaConverter.populateUsermeta(o.metadata.asJava, domObject)
      riakIndexConverter.populateIndexes(buildIndexes(o.binIndexes, o.intIndexes), domObject)
      riakLinksConverter.populateLinks(((o.links map {
        _.list map {
          l ⇒ new RiakLink(l.bucket, l.key, l.tag)
        }
      }) | Nil).asJavaCollection, domObject)
      KeyUtil.setKey(domObject, o.key)
      domObject.successNel
    },
    (o: T) ⇒ {
      val value = Json.generate(o).getBytes()
      val key = KeyUtil.getKey(o, o.key)
      val indexes = riakIndexConverter.getIndexes(o)
      val links = (riakLinksConverter.getLinks(o).asScala map {
        l ⇒ l: ScaliakLink
      }).toList.toNel
      val metadata = usermetaConverter.getUsermetaData(o).asScala.toMap
      val binIndexes = indexes.getBinIndexes().asScala.mapValues(_.asScala.toSet).toMap
      val intIndexes = indexes.getIntIndexes().asScala.mapValues(_.asScala.map(_.intValue()).toSet).toMap

      WriteObject(key, value, contentType = "application/json", metadata = metadata, binIndexes = binIndexes, intIndexes = intIndexes, links = links)
    }
  )

  private def buildIndexes(binIndexes: Map[BinIndex, Set[String]], intIndexes: Map[IntIndex, Set[Int]]): RiakIndexes = {
    val tempBinIndexes: java.util.Map[BinIndex, java.util.Set[String]] = new java.util.HashMap[BinIndex, java.util.Set[String]]()
    val tempIntIndexes: java.util.Map[IntIndex, java.util.Set[java.lang.Integer]] = new java.util.HashMap[IntIndex, java.util.Set[java.lang.Integer]]()
    for {(k, v) ← binIndexes} {
      val set: java.util.Set[String] = new java.util.HashSet[String]()
      v.foreach(set.add(_))
      tempBinIndexes.put(k, set)
    }
    for {(k, v) ← intIndexes} {
      val set: java.util.Set[java.lang.Integer] = new java.util.HashSet[java.lang.Integer]()
      v.foreach(set.add(_))
      tempIntIndexes.put(k, set)
    }

    new RiakIndexes(tempBinIndexes, tempIntIndexes)
  }

  // Default to lower case string representation of the class name => UserProfile: userprofile
  val bucket = clientPool.bucket(proposedBucketName.getOrElse(clazz.getSimpleName.toString.toLowerCase)).unsafePerformIO match {
    case Success(b) ⇒ b
    case Failure(e) ⇒ throw e
  }

  def fetch(key: String) = {
    bucket.fetch(key).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        mbFetched
      }
      case Failure(es) ⇒ throw es.head
    }
  }

  def fetchAsJSON(keys: List[String]) = {
    val mrJob = MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- filterNotFound),
      riakObjects = Some(Map(bucket.name -> keys.toSet)))
    bucket.mapReduce(mrJob)
  }

  def fetch(keys: List[String]) = {
    if (keys.length == 0) {
      Array[T]()
    } else {
      fetchAsJSON(keys).unsafePerformIO match {
        case Success(mbFetched) ⇒ {
          Json.parse[Array[T]](mbFetched)
        }
        case Failure(es) ⇒ throw es
      }
    }
  }

  def fetchObjectsWithIndexByValueAsJSON(index: String, value: String, sortField: Option[String] = None, sortDESC: Boolean = false) = {
    val mrJob = sortField.flatMap {
      field ⇒
        Some(MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)),
          binIndex = Some(BinaryIndex(index, value, bucket.name))))
    }.getOrElse {
      MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson),
        binIndex = Some(BinaryIndex(index, value, bucket.name)))
    }
    bucket.mapReduce(mrJob)
  }

  def fetchObjectsWithIndexByValue(index: String, value: String, sortField: Option[String] = None, sortDESC: Boolean = false) = {
    fetchObjectsWithIndexByValueAsJSON(index, value, sortField, sortDESC).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        Json.parse[Array[T]](mbFetched)
      }
      case Failure(es) ⇒ throw es
    }
  }

  def deleteWithKeys(keys: List[String]) = {
    keys.foreach(key ⇒ delete(key))
  }

  def store(domainObject: T) = {
    bucket.store(domainObject).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        Unit
      }
      case Failure(es) ⇒ throw es.head
    }
  }

  def delete(domainObject: T) = {
    bucket.delete(domainObject).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        Unit
      }
      case Failure(es) ⇒ throw es
    }
  }

  def delete(key: String) = {
    bucket.deleteByKey(key).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        Unit
      }
      case Failure(es) ⇒ throw es
    }
  }

  def deleteWithCheck(domainObject: T, checkFunction: ⇒ T ⇒ Boolean) = {
    if (checkFunction(domainObject))
      delete(domainObject)
    else
      throw new Exception("Could not delete object, check did not succeed")
  }

  def fetchKeysForIndexWithValue(idx: String, idv: String) = {
    bucket.fetchIndexByValue(idx, idv).unsafePerformIO match {
      case Success(mbFetched) ⇒ mbFetched
      case Failure(es) ⇒ throw es
    }
  }

  def fetchKeysForIndexWithValue(idx: String, idv: Int) = {
    bucket.fetchIndexByValue(idx, idv).unsafePerformIO match {
      case Success(mbFetched) ⇒ mbFetched
      case Failure(es) ⇒ throw es
    }
  }
}
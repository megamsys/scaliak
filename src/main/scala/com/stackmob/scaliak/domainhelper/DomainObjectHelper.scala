package com.stackmob.scaliak.domainhelper

import scalaz._
import Scalaz._
import effects._
import com.basho.riak.client.convert._
import com.basho.riak.client.RiakLink
import com.basho.riak.client.query.indexes.{ RiakIndexes, IntIndex, BinIndex }
import com.stackmob.scaliak.mapreduce._
import com.stackmob.scaliak.mapreduce.MapReduceFunctions._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import com.codahale.jerkson.Json
import com.stackmob.scaliak.ScaliakConverter
import com.stackmob.scaliak.ScaliakPbClientPool
import com.stackmob.scaliak.ReadObject
import com.stackmob.scaliak.WriteObject
import com.stackmob.scaliak.ScaliakLink
import com.basho.riak.client.query.MapReduceResult
import com.basho.riak.client.IRiakObject

abstract class DomainObject {
  def key: String
}

abstract class DomainObjectHelper[T <: DomainObject](val clientPool: ScaliakPbClientPool, val proposedBucketName: Option[String] = None)(implicit mot: Manifest[T]) {
  val clazz: Class[T] = mot.erasure.asInstanceOf[Class[T]]

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
    })

  private def buildIndexes(binIndexes: Map[BinIndex, Set[String]], intIndexes: Map[IntIndex, Set[Int]]): RiakIndexes = {
    val tempBinIndexes: java.util.Map[BinIndex, java.util.Set[String]] = new java.util.HashMap[BinIndex, java.util.Set[String]]()
    val tempIntIndexes: java.util.Map[IntIndex, java.util.Set[java.lang.Integer]] = new java.util.HashMap[IntIndex, java.util.Set[java.lang.Integer]]()
    for { (k, v) ← binIndexes } {
      val set: java.util.Set[String] = new java.util.HashSet[String]()
      v.foreach(set.add(_))
      tempBinIndexes.put(k, set)
    }
    for { (k, v) ← intIndexes } {
      val set: java.util.Set[java.lang.Integer] = new java.util.HashSet[java.lang.Integer]()
      v.foreach(set.add(_))
      tempIntIndexes.put(k, set)
    }

    new RiakIndexes(tempBinIndexes, tempIntIndexes)
  }

  def snakify(name: String) = name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase

  // Default to lower case string representation of the class name => UserProfile: userprofile
  val bucket = clientPool.bucket(proposedBucketName.getOrElse(clazz.getSimpleName.toString |> snakify)).unsafePerformIO match {
    case Success(b) ⇒ b
    case Failure(e) ⇒ throw e
  }

  def fetch(key: String) = bucket.fetch(key)

  implicit def mapReduceResultToObjectOptionIterable(mrr: MapReduceResult) = Json.parse[Iterable[Option[T]]](mrr.getResultRaw)

  def fetch(keys: List[String]): IO[Iterable[T]] = {
    val mrJob = MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- filterNotFound),
      riakObjects = Some(Map(bucket.name -> keys.toSet)))
    bucket.mapReduce(mrJob).map { _.map { mapReduceResultToObjectOptionIterable(_) } } map { f ⇒
      f match {
        case Success(s) ⇒ s filter { _.isDefined } map { _.get }
        case Failure(e) ⇒ Iterable[T]()
      }
    }
  }

  def fetchObjectsWithIndexByValue[V](index: String, value: V, sortField: Option[String] = None, sortDESC: Boolean = false): IO[Iterable[T]] = {
    val mrJob = value match {
      case x @ (_: Int | _: Range) ⇒ {
        val iv = x match {
          case y: Int ⇒ Left(y)
          case y: Range ⇒ Right(y)
        }
        val intIndex = Some(IntegerIndex(index, iv, bucket.name))
        sortField.flatMap {
          field ⇒
            Some(MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)), intIndex = intIndex))
        }.getOrElse {
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson), intIndex = intIndex)
        }
      }
      case x: String ⇒ {
        val binIndex = Some(BinaryIndex(index, x, bucket.name))
        sortField.flatMap {
          field ⇒
            Some(MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)), binIndex = binIndex))
        }.getOrElse {
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson), binIndex = binIndex)
        }
      }
      case _ ⇒ throw new Exception("Unknown value type used")
    }

    bucket.mapReduce(mrJob).map { _.map { mapReduceResultToObjectOptionIterable(_) } } map { f ⇒
      f match {
        case Success(s) ⇒ s filter { _.isDefined } map { _.get }
        case Failure(e) ⇒ Iterable[T]()
      }
    }
  }

  def deleteWithKeys(keys: List[String]) = keys.foreach(key ⇒ delete(key))

  def store(domainObject: T) = bucket.store(domainObject, returnBody = true)

  def delete(domainObject: T) = bucket.delete(domainObject)

  def delete(key: String) = bucket.deleteByKey(key)

  def fetchKeysForIndexWithValue(idx: String, idv: String) = bucket.fetchIndexByValue(idx, idv)

  def fetchKeysForIndexWithValue(idx: String, idv: Int) = bucket.fetchIndexByValue(idx, idv)
}

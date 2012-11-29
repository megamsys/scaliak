package com.stackmob.scaliak.domainhelper

import scalaz._
import Scalaz._
import effects._
import com.stackmob.scaliak.mapreduce._
import com.stackmob.scaliak.mapreduce.MapReduceFunctions._

import com.stackmob.scaliak.ScaliakConverter
import com.stackmob.scaliak.ScaliakPbClientPool
import com.basho.riak.client.query.MapReduceResult
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.scalaz._
import net.liftweb.json.scalaz.JsonScalaz._
import net.liftweb.json._
import net.liftweb.json.scalaz.JsonScalaz._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST.JValue


abstract class DomainObjectHelper[T](val clientPool: ScaliakPbClientPool, val bucketname: String)(implicit domainConverter: ScaliakConverter[T], json: JSON[T]) {
  val bucket = clientPool.bucket(bucketname).unsafePerformIO match {
    case Success(b) => b
    case Failure(e) => throw e
  }

  def fetch(key: String) = bucket.fetch(key)

  implicit def mapReduceResultToObjectList(mrr: MapReduceResult): Option[List[T]] = parseOpt(mrr.getResultRaw).flatMap(fromJSON[List[T]](_).toOption)

  def fetch(keys: List[String]): IO[List[T]] = {
    val mrJob = MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- filterNotFound),
      riakObjects = Some(Map(bucket.name -> keys.toSet)))
    bucket.mapReduce(mrJob).map {
      _.map {
        mapReduceResultToObjectList(_)
      }
    } map {
      f =>
        f match {
          case Success(s) => s getOrElse List[T]()
          case Failure(e) => List[T]()
        }
    }
  }

  def fetchObjectsWithIndexByValue[V](index: String, value: V, sortField: Option[String] = None, sortDESC: Boolean = false): IO[List[T]] = {
    val mrJob = value match {
      case x@(_: Int | _: Range) => {
        val iv = x match {
          case y: Int => Left(y)
          case y: Range => Right(y)
        }
        val intIndex = Some(IntegerIndex(index, iv, bucket.name))
        sortField.flatMap {
          field =>
            Some(MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)), intIndex = intIndex))
        }.getOrElse {
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson), intIndex = intIndex)
        }
      }
      case x: String => {
        val binIndex = Some(BinaryIndex(index, x, bucket.name))
        sortField.flatMap {
          field =>
            Some(MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)), binIndex = binIndex))
        }.getOrElse {
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson), binIndex = binIndex)
        }
      }
      case _ => throw new Exception("Unknown value type used")
    }

    bucket.mapReduce(mrJob).map {
      _.map {
        mapReduceResultToObjectList(_)
      }
    } map {
      f =>
        f match {
          case Success(s) => s getOrElse List[T]()
          case Failure(e) => List[T]()
        }
    }
  }

  def deleteWithKeys(keys: List[String]) = keys.foreach(key => delete(key))

  def store(domainObject: T) = bucket.store(domainObject, returnBody = true)

  def delete(domainObject: T) = bucket.delete(domainObject)

  def delete(key: String) = bucket.deleteByKey(key)

  def fetchKeysForIndexWithValue(idx: String, idv: String) = bucket.fetchIndexByValue(idx, idv)

  def fetchKeysForIndexWithValue(idx: String, idv: Int) = bucket.fetchIndexByValue(idx, idv)
}

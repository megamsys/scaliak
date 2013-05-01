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

package com.stackmob.scaliak.domainhelper

import scalaz._
import Scalaz._
import scalaz.effect.IO
import com.stackmob.scaliak.mapreduce._
import com.stackmob.scaliak.mapreduce.MapReduceFunctions._

import com.stackmob.scaliak.{ScaliakBucket, ScaliakConverter, ScaliakPbClientPool}
import com.basho.riak.client.query.MapReduceResult
import net.liftweb.json._
import net.liftweb.json.scalaz.JsonScalaz._

abstract class DomainObjectHelper[T](val clientPool: ScaliakPbClientPool, val bucketname: String)
                                    (implicit domainConverter: ScaliakConverter[T], json: JSON[T]) {
  val bucket = clientPool.bucket(bucketname).unsafePerformIO() valueOr { throw _ }

  def fetch(key: String): IO[ValidationNel[Throwable, Option[T]]] = bucket.fetch(key)

  implicit def mapReduceResultToObjectList(mrr: MapReduceResult): Option[List[T]] = parseOpt(mrr.getResultRaw).flatMap(fromJSON[List[T]](_).toOption)

  def fetch(keys: List[String]): IO[List[T]] = {
    val mrJob = MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- filterNotFound),
      riakObjects = Some(Map(bucket.name -> keys.toSet)))
    bucket.mapReduce(mrJob).map {
      ~_.toOption.map {
        ~mapReduceResultToObjectList(_)
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
        val intIndex = IntegerIndex(index, iv, bucket.name).some
        sortField.some { field =>
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)), intIndex = intIndex)
        } none {
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson), intIndex = intIndex)
        }
      }
      case x: String => {
        val binIndex = BinaryIndex(index, x, bucket.name).some
        sortField.some { field =>
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson(false) |- sort(field, sortDESC)), binIndex = binIndex)
        } none {
          MapReduceJob(mapReducePhasePipe = MapReducePhasePipe(mapValuesToJson), binIndex = binIndex)
        }
      }
      case _ => throw new Exception("Unknown value type used")
    }

    bucket.mapReduce(mrJob).map {
      ~_.toOption.map {
        ~mapReduceResultToObjectList(_)
      }
    }
  }

  def deleteWithKeys(keys: List[String]) {
    keys.foreach(key => delete(key))
  }

  def store(domainObject: T) = bucket.store(domainObject, returnBody = true)

  def delete(domainObject: T) = bucket.delete(domainObject)

  def delete(key: String) = bucket.deleteByKey(key)

  def fetchKeysForIndexWithValue(idx: String, idv: String) = bucket.fetchIndexByValue(idx, idv)

  def fetchKeysForIndexWithValue(idx: String, idv: Int) = bucket.fetchIndexByValue(idx, idv)

}

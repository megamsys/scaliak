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
import java.util.Date
import com.basho.riak.client.api.cap.VClock
import com.basho.riak.client.core.query.BucketProperties
import com.basho.riak.client.core.operations.FetchOperation
import com.basho.riak.client.core.operations.StoreOperation
import com.basho.riak.client.core.operations.StoreBucketPropsOperation

sealed trait ScaliakArgument[T] {
  this: MetaBuilder =>

  def value: Option[T]

}

sealed trait MetaBuilder

trait FetchMetaBuilder[T] extends MetaBuilder {
  this: ScaliakArgument[T] =>

  def addToMeta(builder: FetchOperation.Builder) { value foreach fetchMetaFunction(builder) }

  def fetchMetaFunction(builder: FetchOperation.Builder): T => FetchOperation.Builder
}

trait UpdateBucketBuilder[T] extends MetaBuilder {
  this: ScaliakArgument[T] =>

  def addToMeta(builder: StoreBucketPropsOperation.Builder) { value foreach bucketMetaFunction(builder) }

  def bucketMetaFunction(builder: StoreBucketPropsOperation.Builder): T => StoreBucketPropsOperation.Builder
}

trait StoreMetaBuilder[T] extends MetaBuilder {
  this: ScaliakArgument[T] =>

  def addToMeta(builder: StoreOperation.Builder) { value foreach storeMetaFunction(builder) }
  def storeMetaFunction(builder: StoreOperation.Builder): T => StoreOperation.Builder
}

case class AllowSiblingsArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with UpdateBucketBuilder[Boolean] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withAllowMulti
}

case class LastWriteWinsArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with UpdateBucketBuilder[Boolean] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withLastWriteWins
}

case class NValArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withNVal
}

case class RArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int] with UpdateBucketBuilder[Int] {
  /**
   * TO-DO
   * def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withR
   */
  def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withR(_: Int)
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = {
    meta.withR(_: Int)
  }
}

case class PRArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with FetchMetaBuilder[Int] with UpdateBucketBuilder[Int] {
  def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withPr(_: Int)
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withPr(_: Int)
}

case class NotFoundOkArgument(value: Option[Boolean] = none)
  extends ScaliakArgument[Boolean]
  with FetchMetaBuilder[Boolean]
  with UpdateBucketBuilder[Boolean] {

  def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withNotFoundOK
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withNotFoundOk

}

case class BasicQuorumArgument(value: Option[Boolean] = none)
  extends ScaliakArgument[Boolean]
  with FetchMetaBuilder[Boolean]
  with UpdateBucketBuilder[Boolean] {

  def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withBasicQuorum
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withBasicQuorum

}

case class ReturnDeletedVCLockArgument(value: Option[Boolean] = none) extends ScaliakArgument[Boolean] with FetchMetaBuilder[Boolean] {
  def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withReturnDeletedVClock
  def allowTombstones = value.getOrElse(false)
}

case class IfModifiedVClockArgument(value: Option[Array[Byte]] = none) extends ScaliakArgument[Array[Byte]] with FetchMetaBuilder[Array[Byte]] {
  def fetchMetaFunction(meta: FetchOperation.Builder) = meta.withIfNotModified(_: Array[Byte])

}

case class WArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] with StoreMetaBuilder[Int] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withW(_: Int)
  def storeMetaFunction(meta: StoreOperation.Builder) = meta.withW(_:Int)

}

case class RWArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withRw(_: Int)
}

case class DWArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] with StoreMetaBuilder[Int] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withDw(_: Int)
  def storeMetaFunction(meta: StoreOperation.Builder) = meta.withDw(_:Int)

}

case class PWArgument(value: Option[Int] = none) extends ScaliakArgument[Int] with UpdateBucketBuilder[Int] with StoreMetaBuilder[Int] {
  def bucketMetaFunction(meta: StoreBucketPropsOperation.Builder) = meta.withPw(_: Int)
  def storeMetaFunction(meta: StoreOperation.Builder) = meta.withPw(_:Int)

}

case class ReturnBodyArgument(value: Option[Boolean] = Option(false)) extends ScaliakArgument[Boolean] with StoreMetaBuilder[Boolean] {
  def storeMetaFunction(meta: StoreOperation.Builder) = meta.withReturnBody(_:Boolean)

}

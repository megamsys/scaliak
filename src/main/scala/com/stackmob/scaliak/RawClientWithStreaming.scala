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

import scalaz.effect.IO
import com.basho.riak.client.core.operations._
import com.basho.riak.client.core.query.{ BucketProperties, Namespace, RiakObject }
import com.basho.riak.client.core.RiakFuture
import scala.concurrent.{ Promise, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.duration.{ Duration, Deadline }
import scala.util.{ Try, Success, Failure }
import scala.util.control.Exception._
import scala.collection.JavaConverters._

trait RawClientWithStreaming {

  def ping(): Try[Void]

  def listBuckets(bucketType: String = Namespace.DEFAULT_BUCKET_TYPE): Try[List[String]]

  def fetchBucket(f: FetchBucketPropsOperation): Try[BucketProperties]

  def storeBucket(p: StoreBucketPropsOperation): Try[Void]

  def fetch(f: FetchOperation): Try[FetchOperation.Response]

  def store(s: StoreOperation): Try[StoreOperation.Response]

  def fetchIndex(s: SecondaryIndexQueryOperation): Try[List[String]]

  def delete(d: DeleteOperation): Try[Void]

  def shutdown(): Try[Boolean]

  private def mkRiakFutureToComplete[T, S](rFuture: RiakFuture[T, S],
    maybeTimeout: Option[Duration] = None): Try[T] = {

    val maybeDeadline = maybeTimeout.map(_.toNanos nanos fromNow)

    if (maybeDeadline.exists(_.isOverdue)) rFuture.cancel(true)

    val completed_res = if (!rFuture.isDone || !rFuture.isCancelled) {
      val rFutureCompleted = rFuture.get
      if (rFuture.cause != null) Failure(rFuture.cause) else Success(rFutureCompleted)
    } else Failure(new RuntimeException("""Dory: Just keep "swimming". operation already done or cancelled."""))
    completed_res
  }

  implicit def riakFuture2ScalaTry[T, S](f: RiakFuture[T, S]): Try[T] = mkRiakFutureToComplete[T, S](f)

  // def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U, iter: IterV[U, A]): IO[IterV[U, A]]

}


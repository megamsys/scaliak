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
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.query.BucketProperties
import com.basho.riak.client.core.query.Namespace


trait RawClientWithStreaming {
  //  def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U, iter: IterV[U, A]): IO[IterV[U, A]]
  def ping() 
  
  def listBuckets(bucketType: String = Namespace.DEFAULT_BUCKET_TYPE): List[String]
  
  def fetchBucket(f: FetchBucketPropsOperation): BucketProperties
  
  def storeBucket(p: StoreBucketPropsOperation)

  def fetch(f: FetchOperation): FetchOperation.Response
  
  def store(s: StoreOperation): StoreOperation.Response

  def fetchIndex(s: SecondaryIndexQueryOperation): List[String]

  def delete(d: DeleteOperation)

  def shutdown()

}

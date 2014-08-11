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
import scalaz.syntax.monad._
import com.fasterxml.jackson.databind.ObjectMapper
import com.basho.riak.client.core.operations._
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.{ RiakCluster, RiakNode }
import com.basho.riak.client.api.commands.StoreBucketProperties
import com.basho.riak.client.core.query.BucketProperties
import com.basho.riak.client.api.commands.indexes.{ SecondaryIndexQuery }
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.util.BinaryValue

import annotation.tailrec
import scala.collection.JavaConverters._
import java.util.LinkedList

class PBStreamingClient(hosts: List[String], port: Int) extends RawClientWithStreaming {

  val cluster: RiakCluster = {
    val nodes = RiakNode.Builder.buildNodes(
      (new RiakNode.Builder()
        withRemotePort (port)
        withMinConnections (10)), new LinkedList(hosts.asJava))
    val cluster = new RiakCluster.Builder(nodes).build()
    cluster.start()
    cluster
  }

  def ping(): Unit = {
    val ping = new PingOperation()
    cluster.execute(ping)
    ping.await()
  }

  def listBuckets(bucketType: String = Namespace.DEFAULT_BUCKET_TYPE): List[String] = {
    val listOp = new ListBucketsOperation.Builder()
      .withBucketType(BinaryValue.createFromUtf8(bucketType))
      .build()
    cluster.execute(listOp)
    listOp.get().getBuckets().asScala.toList.map(_.toString)
  }

  def fetchBucket(op: FetchBucketPropsOperation): BucketProperties = {
    cluster.execute(op)
    op.get().getBucketProperties()
  }

  def storeBucket(op: StoreBucketPropsOperation) = {
    cluster.execute(op)
    op.get()
    // if (op.isSuccess && op.isDone else op.cause)
  }

  def fetch(op: FetchOperation): FetchOperation.Response = {
    cluster.execute(op)
    val r = op.get()
    r
  }

  def store(op: StoreOperation): StoreOperation.Response = {
    cluster.execute(op)
    op.get()
  }

  def fetchIndex(op: SecondaryIndexQueryOperation): List[String] = {
    cluster.execute(op)
    op.get().getEntryList.asScala.toList map (_.getObjectKey.toString)
  }

  def delete(op: DeleteOperation) = {
    cluster.execute(op);
    op.get();
  }

  def shutdown(): Unit = {
    cluster.shutdown()
  }

  /*
    *
    *  val mapper = new ObjectMapper()
    *  override def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U,  iter: IterV[U, A]): IO[IterV[U, A]] = {
    val meta = new RequestMeta()
    meta.contentType(Constants.CTYPE_JSON)
    val source = pbClient.mapReduce(spec.getJSON, meta)

    def deserialize(resp: MapReduce.Response): T = {
      mapper.readValue[java.util.Collection[T]](
        resp.getJSON.toString,
        mapper.getTypeFactory.constructCollectionType(classOf[java.util.Collection[_]], elementClass)
      ).asScala.head
    }

    @tailrec
    def feedFromSource(source: MapReduceResponseSource, iter: IterV[U, A]): IO[IterV[U, A]] = iter match {
      case _ if source.isClosed => iter.pure[IO]
      case Done(_, _) => iter.pure[IO]
      case Cont(k) if !source.hasNext => feedFromSource(source, k(Empty[U]))
      case Cont(k) => {
        val next = source.next()
        if(Option(next.getJSON).isDefined) {
          feedFromSource(source, k(El(converter(deserialize(next)))))
        } else {
          iter.pure[IO]
        }
      }

    }

    feedFromSource(source, iter)
  }
*/

}

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
import com.basho.riak.client.api._
import com.basho.riak.client.api.commands.StoreBucketProperties
import com.basho.riak.client.api.commands.indexes.{ SecondaryIndexQuery }

import com.basho.riak.client.core.operations._
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.{ RiakCluster, RiakNode }
import com.basho.riak.client.core.query.BucketProperties
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.util.BinaryValue

import annotation.tailrec
import scala.util.{ Try, Success, Failure }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

class PBStreamingClient(hosts: List[String], port: Int) extends RawClientWithStreaming {

  val nodes = Try({
    RiakNode.Builder.buildNodes(
      (new RiakNode.Builder()
        withRemotePort (port)
        withMinConnections (5)), new java.util.LinkedList(hosts.asJava))
  })

  lazy val cluster: Try[RiakCluster] = for {
    suc_nodes <- nodes
    suc_clust <- Try(new RiakCluster.Builder(suc_nodes).build())
  } yield {
    suc_clust.start
    suc_clust
  }

  def ping(): Try[Void] = cluster flatMap (_.execute(new PingOperation()))

  def listBuckets(bucketType: String = Namespace.DEFAULT_BUCKET_TYPE): Try[List[String]] = {
    cluster flatMap ({ x: RiakCluster =>
      {
        val listOp = new ListBucketsOperation.Builder()
          .withBucketType(BinaryValue.createFromUtf8(bucketType))
          .build()
        x.execute(listOp)
      }
    }) match {
      case Success(s) => Success(s.getBuckets().asScala.toList.map(_.toString))
      case Failure(t) => Failure(t)
    }
  }

  def fetchBucket(fbop: FetchBucketPropsOperation): Try[BucketProperties] = {
    cluster flatMap ({ x: RiakCluster =>
      x.execute(fbop)
    }) match {
      case Success(s) => Success(s.getBucketProperties())
      case Failure(t) => Failure(t)
    }
  }

  def fetch(foop: FetchOperation): Try[FetchOperation.Response] = cluster flatMap (_.execute(foop))

  def storeBucket(sbop: StoreBucketPropsOperation): Try[Void] = cluster flatMap (_.execute(sbop))

  def store(stop: StoreOperation): Try[StoreOperation.Response] = cluster flatMap (_.execute(stop))

  def fetchIndex(fiop: SecondaryIndexQueryOperation): Try[List[String]] = {
    cluster flatMap ({ x: RiakCluster =>
      x.execute(fiop)
    }) match {
      case Success(s) => Success(s.getEntryList.asScala.toList map (_.getObjectKey.toString))
      case Failure(t) => Failure(t)
    }    
  }

  def delete(deop: DeleteOperation): Try[Void] = cluster flatMap (_.execute(deop))

  def shutdown(): Try[Boolean] = cluster flatMap ({ x: RiakCluster =>
    val res: Try[java.lang.Boolean] = Try(x.shutdown().get)
    res.map(Boolean.unbox(_))
  })

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

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

import com.basho.riak.client.raw.pbc.PBClientAdapter
import com.basho.riak.pbc.{RiakClient => PBRiakClient, MapReduceResponseSource, RequestMeta}
import com.basho.riak.client.raw.query.MapReduceSpec
import com.basho.riak.pbc.mapreduce.MapReduceResponse
import com.basho.riak.client.http.util.Constants
import scalaz.IterV._
import scalaz.{Empty => _, _}
import effects.IO
import Scalaz._
import com.fasterxml.jackson.databind.ObjectMapper
import annotation.tailrec
import scala.collection.JavaConverters._

class PBStreamingClient(host: String, port: Int) extends PBClientAdapter(host, port) with RawClientWithStreaming {

  val pbClient = new PBRiakClient(host, port)
  val mapper = new ObjectMapper()

  override def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U,  iter: IterV[U, A]): IO[IterV[U, A]] = {
    val meta = new RequestMeta()
    meta.contentType(Constants.CTYPE_JSON)
    val source = pbClient.mapReduce(spec.getJSON, meta)

    def deserialize(resp: MapReduceResponse): T = {
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

}

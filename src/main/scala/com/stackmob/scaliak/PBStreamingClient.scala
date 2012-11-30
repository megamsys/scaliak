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
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.`type`.TypeFactory

/**
 * Created with IntelliJ IDEA.
 * User: drapp
 * Date: 11/27/12
 * Time: 12:34 AM
 */
class PBStreamingClient(host: String, port: Int) extends PBClientAdapter(host, port) with RawClientWithStreaming {

  val pbClient = new PBRiakClient(host, port)
  val mapper = new ObjectMapper()

  def mapReduce[T, A](spec: MapReduceSpec, elementClass: Class[T], iter: IterV[T, A]): IO[IterV[T, A]] = {
    val meta = new RequestMeta()
    meta.contentType(Constants.CTYPE_JSON)
    val source = pbClient.mapReduce(spec.getJSON, meta)

    def deserialize(resp: MapReduceResponse): T = mapper.readValue(resp.getJSON.toString, TypeFactory.`type`(elementClass))

    def feedFromSource(source: MapReduceResponseSource, iter: IterV[T, A]): IO[IterV[T, A]] = iter match {
      case _ if source.isClosed => iter.pure[IO]
      case Done(_, _) => iter.pure[IO]
      case Cont(k) if !source.hasNext => feedFromSource(source, k(Empty[T]))
      case Cont(k) => source.next().pure[IO].flatMap(next => feedFromSource(source, k(El(deserialize(next)))))
    }

    feedFromSource(source, iter)
  }

}

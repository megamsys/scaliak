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
import annotation.tailrec
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * User: drapp
 * Date: 11/27/12
 * Time: 12:34 AM
 */
class PBStreamingClient(host: String, port: Int) extends PBClientAdapter(host, port) with RawClientWithStreaming {

  val pbClient = new PBRiakClient(host, port)
  val mapper = new ObjectMapper()

  def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T],  converter: T => U,  iter: IterV[U, A]): IO[IterV[U, A]] = {
    val meta = new RequestMeta()
    meta.contentType(Constants.CTYPE_JSON)
    val source = pbClient.mapReduce(spec.getJSON, meta)

    def deserialize(resp: MapReduceResponse): T = mapper.readValue[java.util.Collection[T]](resp.getJSON.toString, TypeFactory.collectionType(classOf[java.util.Collection[_]], elementClass)).asScala.head

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

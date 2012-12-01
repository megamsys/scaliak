package com.stackmob.scaliak

import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.raw.query.indexes.IndexQuery
import scalaz.IterV
import scalaz.effects.IO
import com.basho.riak.client.raw.query.MapReduceSpec

/**
 * Created with IntelliJ IDEA.
 * User: drapp
 * Date: 11/28/12
 * Time: 4:06 PM
 */
class HTTPStreamingClient(url: String) extends HTTPClientAdapter(url) with RawClientWithStreaming {
  def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U, iter: IterV[U, A]): IO[IterV[U, A]] = {
    throw new UnsupportedOperationException("Streaming mapreduce not supported in HTTP")
  }
}

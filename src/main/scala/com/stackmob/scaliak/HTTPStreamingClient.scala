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
  def mapReduce[T, A](spec: MapReduceSpec, elementClass: Class[T], iter: IterV[T, A]): IO[IterV[T, A]] = {
    throw new UnsupportedOperationException("Streaming mapreduce not supported in HTTP")
  }
}

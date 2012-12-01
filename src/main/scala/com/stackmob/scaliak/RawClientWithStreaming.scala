package com.stackmob.scaliak

import com.basho.riak.client.raw.RawClient
import com.basho.riak.client.raw.query.indexes.IndexQuery
import scalaz.IterV
import com.basho.riak.client.raw.query.MapReduceSpec
import com.basho.riak.pbc.mapreduce.MapReduceResponse
import scalaz.effects.IO

/**
 * Created with IntelliJ IDEA.
 * User: drapp
 * Date: 11/28/12
 * Time: 2:17 PM
 */
trait RawClientWithStreaming extends RawClient {

  def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U, iter: IterV[U, A]): IO[IterV[U, A]]
}

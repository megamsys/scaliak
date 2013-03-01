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

import com.basho.riak.client.raw.http.HTTPClientAdapter
import scalaz.IterV
import scalaz.effects.IO
import com.basho.riak.client.raw.query.MapReduceSpec

class HTTPStreamingClient(url: String) extends HTTPClientAdapter(url) with RawClientWithStreaming {
  override def mapReduce[T, U, A](spec: MapReduceSpec, elementClass: Class[T], converter: T => U, iter: IterV[U, A]): IO[IterV[U, A]] = {
    throw new UnsupportedOperationException("Streaming mapreduce not supported in HTTP")
  }
}

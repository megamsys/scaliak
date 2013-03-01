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

object Scaliak {

  def httpClient(url: String): ScaliakClient = {
    val rawClient = new HTTPStreamingClient(url)
    new ScaliakClient(rawClient, None)
  }

  // PB Client is a lot faster, but we'll still need the HTTP client for getting bucket properties etc.
  def pbClient(host: String, port: Int, httpPort: Int): ScaliakClient = {
    val rawClient = new PBStreamingClient(host, port)
    val secHTTPClient = new HTTPClientAdapter("http://" + host + ":" + httpPort + "/riak")
    new ScaliakClient(rawClient, Some(secHTTPClient))
  }

}

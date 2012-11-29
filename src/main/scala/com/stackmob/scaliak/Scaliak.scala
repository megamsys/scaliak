package com.stackmob.scaliak

import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.pbc.{RiakClient => PBRiakClient}
import com.basho.riak.client.raw.pbc.PBClientAdapter

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/18/11
 * Time: 1:38 PM
 */

object Scaliak {

  def httpClient(url: String): ScaliakClient = {
    val rawClient = new HTTPStreamingClient(url)
    new ScaliakClient(rawClient)
  }

  def pbClient(host: String, port: Int): ScaliakClient = {
    val rawClient = new PBStreamingClient(host, port)
    new ScaliakClient(rawClient)
  }

}
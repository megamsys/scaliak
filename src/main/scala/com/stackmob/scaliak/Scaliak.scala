package com.stackmob.scaliak

import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.raw.pbc.PBClientAdapter

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/18/11
 * Time: 1:38 PM
 */

object Scaliak {

  def httpClient(url: String): ScaliakClient = {
    val rawClient = new HTTPClientAdapter(url)
    new ScaliakClient(rawClient, None)
  }
  
  // PB Client is a lot faster, but we'll still need the HTTP client for getting bucket properties etc.
  def pbClient(host: String, port: Int, httpPort: Int): ScaliakClient = {
    val rawClient = new PBClientAdapter(host, port)
    val secHTTPClient = new HTTPClientAdapter("http://" + host + ":" + httpPort + "/riak")
    new ScaliakClient(rawClient, Some(secHTTPClient))
  }

}
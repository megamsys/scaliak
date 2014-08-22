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

import com.stackmob.scaliak._
import com.basho.riak.client.core.{ RiakCluster, RiakNode }
import scala.util.{ Try, Success, Failure }
import scala.collection.JavaConverters._

object Scaliak {

  private lazy val min_active_connections = 10

  val mkCluster: LiftCluster = { p: Tuple2[List[String], Int] =>    {
      val nodes = Try({
        RiakNode.Builder.buildNodes(
          (new RiakNode.Builder()
            withRemotePort (p._2)
            withMinConnections (min_active_connections)), new java.util.LinkedList(p._1.asJava))
      })

      val cluster: MaybeCluster = for {
        suc_nodes <- nodes
        suc_clust <- Try(new RiakCluster.Builder(suc_nodes).build())
      } yield {
        suc_clust.start
        suc_clust
      }
      cluster
    }
  }

  val mkPBPool: MaybeCluster => ScaliakPbClientPool = { p: MaybeCluster => new ScaliakPbClientPool(p) }

  def clientPool(hosts: List[String], port: Int = RiakNode.Builder.DEFAULT_REMOTE_PORT) = mkPBPool(mkCluster(Tuple2[List[String], Int](hosts, port)))

  def client(hosts: List[String], port: Int = RiakNode.Builder.DEFAULT_REMOTE_PORT): ScaliakClient = {
    val rawClient = new PBStreamingClient(mkCluster(Tuple2[List[String], Int](hosts, port)))
    new ScaliakClient(rawClient)
  }

}
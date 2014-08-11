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

import scalaz._
import Scalaz._
import scalaz.effect.IO
import org.apache.commons.pool2._
import org.apache.commons.pool2.impl._
import scala.collection.immutable.List
import java.io.IOException
import scala.collection.JavaConversions._

abstract class ScaliakClientPool {
  def withClient[T](body: RawClientWithStreaming => T): T
}

class ScaliakPbClientFactory(hosts: List[String], port: Int) extends PooledObjectFactory[Object] {

  override def makeObject(): PooledObject[Object] = new DefaultPooledObject(new PBStreamingClient(hosts, port))

  override def destroyObject(sc: org.apache.commons.pool2.PooledObject[Object]) {
    sc.getObject.asInstanceOf[RawClientWithStreaming].shutdown()
  }

  override def passivateObject(sc: org.apache.commons.pool2.PooledObject[Object]) {}

  override def validateObject(sc: org.apache.commons.pool2.PooledObject[Object]): Boolean = {
    try {
      /**
       * TO-DO
       *  Do you want this ?
       *    sc.asInstanceOf[RawClientWithStreaming].ping
       */
      true
    } catch {
      case e: Throwable => false
    }
  }

  override def activateObject(sc: org.apache.commons.pool2.PooledObject[Object]) {}

}

class ScaliakPbClientPool(hosts: List[String], port: Int, httpPort: Int) extends ScaliakClientPool with ScaliakBoot {
  
  val scaliakPbFactory = new ScaliakPbClientFactory(hosts, port);

  val pool = new GenericObjectPool(scaliakPbFactory)

  def pb = scaliakPbFactory.makeObject.getObject.asInstanceOf[RawClientWithStreaming]
  
  def rawOrPool = Right(this)

  def withClient[T](body: RawClientWithStreaming => T): T = {
    val client = pool.borrowObject.asInstanceOf[RawClientWithStreaming]
    val response = body(client)
    pool.returnObject(client)
    response
  }

  def close() {
    pool.close()
  }
  
  override def toString = hosts + ":" + String.valueOf(port)
}

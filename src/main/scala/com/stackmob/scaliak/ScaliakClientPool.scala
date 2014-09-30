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

abstract class ScaliakClientPool extends ScaliakBoot {
  def withClient[T](body: RawClientWithStreaming => T): T
}

class ScaliakPbClientFactory(maybeCluster: MaybeCluster) extends PooledObjectFactory[Object] {


  override def makeObject(): PooledObject[Object] = {
    new DefaultPooledObject[Object](new PBStreamingClient(maybeCluster)) }

  /*is invoked on every instance that has been passivated before it is borrowed from the pool.*/
  override def activateObject(sc: org.apache.commons.pool2.PooledObject[Object]) {
    }

  /*may be invoked on activated instances to make sure they can be borrowed from the pool.*/
  override def validateObject(sc: org.apache.commons.pool2.PooledObject[Object]): Boolean = {
    try {
      // sc.asInstanceOf[RawClientWithStreaming].ping
      true
    } catch {
      case e: Throwable => false
    }
  }

  /*is invoked on every instance when it is returned to the pool.*/
  override def passivateObject(sc: org.apache.commons.pool2.PooledObject[Object]) {

  }

  /*is invoked on every instance when it is being "dropped" from the pool (whether due to the response from validateObject(org.apache.commons.pool2.PooledObject<T>),
    or for reasons specific to the pool implementation.) There is no guarantee that the instance being destroyed will be considered active, passive or in a generally
    consistent state.*/
  override def destroyObject(sc: org.apache.commons.pool2.PooledObject[Object]) {
    sc.getObject.asInstanceOf[RawClientWithStreaming].shutdown()
  }

}

class ScaliakPbClientPool(maybeCluster: MaybeCluster) extends ScaliakClientPool {

  val scaliakPbFactory = new ScaliakPbClientFactory(maybeCluster)

  val pool = new GenericObjectPool[Object](scaliakPbFactory)

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

  override def toString = maybeCluster.toString
}

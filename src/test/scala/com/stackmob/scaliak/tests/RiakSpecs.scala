/*
** Copyright [2013-2014] [Megam Systems]
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/
package com.stackmob.scaliak.tests

import org.specs2.Specification
import org.specs2.specification.Step
import org.specs2.specification.core.{ Fragments }
import com.stackmob.scaliak._

/**
 * @author ram
 *
 */
object riakSetup {

  lazy val sclient  = Scaliak.client(List("127.0.0.1"))
  lazy val sclientp = Scaliak.clientPool(List("127.0.0.1"))

  lazy val rawClientOrClientPool: Either[RawClientWithStreaming, ScaliakClientPool] = sclientp.rawOrPool
  lazy val faultyClientOrClientPool: Either[RawClientWithStreaming, ScaliakClientPool] = Scaliak.client(List("127.0.22.1")).rawOrPool

  val testBucket = "specsbucket"
  val testKey = "specskey2"
  val testKey1 = "specskey1"
  val testContentType = "text/plain; charset=UTF-8"

  lazy val bucket = sclientp.bucket(testBucket).unsafePerformIO().toOption

  def runOnFailureClient[A](f: RawClientWithStreaming => A): A = {
    faultyClientOrClientPool match {
      case Left(client) => f(client)
      case Right(pool)  => pool.withClient[A](f)
    }
  }

  def runOnClient[A](f: RawClientWithStreaming => A): A = {
    rawClientOrClientPool match {
      case Left(client) => f(client)
      case Right(pool)  => pool.withClient[A](f)
    }
  }

  def shutdown() = runOnClient(_.shutdown())
}

trait RiakSpecs extends Specification {

  lazy val riak = riakSetup

  override def map(fs: => Fragments) = Step(riak.sclientp) ^ fs ^ Step(riak.shutdown())


}

trait RiakWithBucketSpecs extends Specification {

  lazy val riak = riakSetup

  override def map(fs: => Fragments) = Step(riak.sclientp) ^ Step(riak.bucket) ^ fs ^ Step(riak.shutdown())
}

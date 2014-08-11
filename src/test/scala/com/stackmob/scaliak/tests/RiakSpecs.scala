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
import org.specs2.specification.{ Step, Fragments }
import com.stackmob.scaliak._

/**
 * @author ram
 *
 */
object riakSetup {

  lazy val client = Scaliak.client(List("127.0.0.1"))

  val testBucket = "specsbucket"
  val testKey = "specskey2"
  val testKey1 = "specskey1"
  val testContentType = "text/plain; charset=UTF-8"

  lazy val bucket = client.bucket(testBucket).unsafePerformIO().toOption

  def shutdown() = client.pb.shutdown()
}

trait RiakSpecs extends Specification {

  lazy val riak = riakSetup

  override def map(fs: => Fragments) = Step(riak.client) ^ fs ^ Step(riak.shutdown())
}

trait RiakWithBucketSpecs extends Specification {

  lazy val riak = riakSetup

  override def map(fs: => Fragments) = Step(riak.client) ^ Step(riak.bucket) ^ fs ^ Step(riak.shutdown())
}
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
package example

import scalaz._
import Scalaz._
import scalaz.effect.IO
import org.slf4j.LoggerFactory

object BasicUsage extends App {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  val client = Scaliak.httpClient("http://localhost:8091/riak")
  client.generateAndSetClientId() // always calls this or setClientId(Array[Byte]) after creating a client

  val bucket = client.bucket("scaliak-example").unsafePerformIO() valueOr { throw _ }

  // Store an object with no conversion
  // this is not the suggested way to use the client
  val key = "somekey"

  // for now this is the only place where null is allowed, because this is not
  // the suggested interface an exception is made. In this case we are storing an object
  // that only exists in memory so we have no vclock.
  val obj = new ReadObject(key, bucket.name, "text/plain", null, "test value".getBytes)
  bucket.store(obj).unsafePerformIO()

  // fetch an object with no conversion
  bucket.fetch(key).unsafePerformIO() match {
    case Success(mbFetched) => logger.debug(mbFetched some { _.stringValue } none { "did not find key" })
    case Failure(es) => throw es.head
  }

  // or you can take advantage of the IO Monad
  def printFetchRes(v: ValidationNel[Throwable, Option[ReadObject]]): IO[Unit] = v match {
    case Success(mbFetched) => {
      logger.debug(
        mbFetched some { "fetched: " + _.stringValue } none { "key does not exist" }
      ).pure[IO]
    }
    case Failure(es) => {
      (es.foreach(e => logger.warn(e.getMessage))).pure[IO]
    } 
  }

  val originalResult = for {
    mbFetchedOrErrors <- bucket.fetch(key)
    _ <- printFetchRes(mbFetchedOrErrors)
    _ <- logger.debug("deleting").pure[IO]
    _ <- bucket.deleteByKey(key)
  } yield (mbFetchedOrErrors.toOption | none)
  originalResult.unsafePerformIO()
 
}

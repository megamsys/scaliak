package com.stackmob.scaliak
package example

import scalaz._
import Scalaz._
import effects._ // not necessary unless you want to take advantage of IO monad

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/16/11
 * Time: 11:15 AM
 */

object BasicUsage extends App {

  val client = Scaliak.httpClient("http://localhost:8091/riak")
  client.generateAndSetClientId() // always calls this or setClientId(Array[Byte]) after creating a client

  val bucket = client.bucket("scaliak-example").unsafePerformIO ||| { throw _ }


  // Store an object with no conversion
  // this is not the suggested way to use the client
  val key = "somekey"

  // for now this is the only place where null is allowed, because this is not
  // the suggested interface an exception is made. In this case we are storing an object
  // that only exists in memory so we have no vclock.
  val obj = new ReadObject(key, bucket.name, "text/plain", null, "test value".getBytes)
  bucket.store(obj).unsafePerformIO  

  // fetch an object with no conversion
  bucket.fetch(key).unsafePerformIO match {
    case Success(mbFetched) => println(mbFetched some { _.stringValue } none { "did not find key" })
    case Failure(es) => throw es.head
  }

  // or you can take advantage of the IO Monad
  def printFetchRes(v: ValidationNEL[Throwable, Option[ReadObject]]): IO[Unit] = v match {
    case Success(mbFetched) => {
      println(
        mbFetched some { "fetched: " + _.stringValue } none { "key does not exist" }
      ).pure[IO]
    }
    case Failure(es) => {
      (es foreach println).pure[IO]
    } 
  }

  val originalResult = for {
    mbFetchedOrErrors <- bucket.fetch(key)
    _ <- printFetchRes(mbFetchedOrErrors)
    _ <- println("deleting").pure[IO]
    _ <- bucket.deleteByKey(key)
  } yield (mbFetchedOrErrors.toOption | none)
  println(originalResult.unsafePerformIO)
 
}
package com.stackmob.scaliak
package example

import scalaz._
import Scalaz._
import effects._ // not necessary unless you want to take advantage of IO monad

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/16/11
 * Time: 2:00 PM 
 */

case class SomeDomainObject(val key: String, val value: String)
object SomeDomainObject {

  implicit val domainConverter: ScaliakConverter[SomeDomainObject] = ScaliakConverter.newConverter[SomeDomainObject](
    (o: ReadObject) => new SomeDomainObject(o.key, o.stringValue).successNel,
    (o: SomeDomainObject) => WriteObject(o.key, o.value.getBytes)
  )

}

object DomainObjects extends App {
  import SomeDomainObject._ // put the implicits at a higher priority scope

  val client = Scaliak.httpClient("http://127.0.0.1:8098/riak")
  client.generateAndSetClientId()

  val bucket = client.bucket("scaliak-example").unsafePerformIO match {
    case Success(b) => b
    case Failure(e) => throw e
  }

  
  // store a domain object
  val key = "some-key"
  if (bucket.store(new SomeDomainObject(key, "value")).unsafePerformIO.isFailure) {
    throw new Exception("failed to store object")
  }
  
  // fetch a domain object
  val fetchResult: ValidationNEL[Throwable, Option[SomeDomainObject]] = bucket.fetch(key).unsafePerformIO
  fetchResult match {
    case Success(mbFetched) => {
      println(mbFetched some { v => v.key + ":" + v.value } none { "did not find key" })
    }
    case Failure(es) => throw es.head
  }

  def printFetchRes(v: ValidationNEL[Throwable, Option[SomeDomainObject]]): IO[Unit] = v match {
    case Success(mbFetched) => {
      println(
        mbFetched some { "fetched: " + _.toString } none { "key does not exist" }
      ).pure[IO]
    }
    case Failure(es) => {
      (es foreach println).pure[IO]
    }
  }    

  // taking advantage of the IO monad
  val action = bucket.fetch(key) flatMap { r =>
    (r.toOption | none) some { obj =>
      for {
        _ <- printFetchRes(r)
        _ <- println("deleting").pure[IO]
        _ <- bucket.delete(obj)
        _ <- println("deleted").pure[IO]
      } yield ()
    } none {
      println("no object to delete").pure[IO]
    }
  }
  
  action.unsafePerformIO
}
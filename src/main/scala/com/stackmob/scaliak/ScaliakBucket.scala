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
//import scalaz.Validation.FlatMap._
import scalaz.effect.IO

import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace

import com.basho.riak.client.core.query.functions.Function
import com.basho.riak.client.core.operations.FetchOperation
import com.basho.riak.client.core.operations.StoreOperation
import com.basho.riak.client.core.operations.DeleteOperation
import com.basho.riak.client.core.operations.ListKeysOperation
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.client.core.query.RiakObject

import scala.collection.JavaConverters._
import scala.util.Try
import com.basho.riak.client.api.cap.{ UnresolvedConflictException, Quorum }
import com.basho.riak.client.core.query.indexes.{ StringBinIndex, LongIntIndex }
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation
import com.basho.riak.client.api.commands.indexes.{ IntIndexQuery, BinIndexQuery, SecondaryIndexQuery }
import mapreduce._

class ScaliakBucket(rawClientOrClientPool: Either[RawClientWithStreaming, ScaliakClientPool],
  val name: String,
  val allowSiblings: Boolean,
  val lastWriteWins: Boolean,
  val nVal: Int,
  val backend: Option[String],
  val smallVClock: Long,
  val bigVClock: Long,
  val youngVClock: Long,
  val oldVClock: Long,
  val precommitHooks: Seq[Function],
  val postcommitHooks: Seq[Function],
  val rVal: Quorum,
  val wVal: Quorum,
  val rwVal: Quorum,
  val dwVal: Quorum,
  val prVal: Quorum,
  val pwVal: Quorum,
  val basicQuorum: Boolean,
  val notFoundOk: Boolean,
  val chashKeyFunction: Function,
  val linkWalkFunction: Function,
  val isSearchable: String) {

  val ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, name)

  def runOnClient[A](f: RawClientWithStreaming => A): A = {
    rawClientOrClientPool match {
      case Left(client) => f(client)
      case Right(pool)  => pool.withClient[A](f)
    }
  }

  /**
   * Warning: Basho advises not to run this operation in production
   * without care because its extremely expensive
   *
   * Lists all the keys in the bucket. A stream is returned to preserve
   * the keys for subsequent iterations which wasn't always the case
   * when backed by a streaming http response in the java client.
   * This is more expensive but once again run this operation w/ care
   */

  def listKeys(): IO[Validation[Throwable, Stream[String]]] = {
    //TO-DO : todo.{map _}.get is actually Try.get. We need to handle exception.
    val lkb = new ListKeysOperation.Builder(ns).build()
    runOnClient(_.listKeys(lkb)).pure[IO].map { todo => { todo.map { _.getKeys.asScala.toStream.map(_.toString) }.get }.success[Throwable] }.except {
      _.failure[Stream[String]].pure[IO]
    }
   }

  /*
   * Creates an IO action that fetches as object by key
   * The action has built-in exception handling that
   * returns a failure with the exception as the only
   * member of the exception list. For custom exception
   * handling see fetchDangerous
   */
  def fetch[T](key: String,
    r: RArgument = RArgument(),
    pr: PRArgument = PRArgument(),
    notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
    returnDeletedVClock: ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(),
    ifModified: IfModifiedVClockArgument = IfModifiedVClockArgument())(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNel[Throwable, Option[T]]] = {
    fetchDangerous(key, r, pr, notFoundOk, basicQuorum, returnDeletedVClock, ifModified) except {
      _.failureNel[Option[T]].pure[IO]
    }

  }

  /*
   * Creates an IO action that fetches an object by key and has no built-in exception handling
   * If using this method it is necessary to deal with exception handling
   * using either the built in facilities in IO or standard try/catch
   */
  def fetchDangerous[T](key: String,
    r: RArgument = RArgument(),
    pr: PRArgument = PRArgument(),
    notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
    returnDeletedVClock: ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(),
    ifModified: IfModifiedVClockArgument = IfModifiedVClockArgument())(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNel[Throwable, Option[T]]] = {
    rawFetch(key, r, pr, notFoundOk, basicQuorum, returnDeletedVClock, ifModified) map {
      riakFetchResponseToResult(key, _, returnDeletedVClock.allowTombstones)
    }

  }

  /*
   * Same as calling fetch and immediately calling unsafePerformIO()
   * Because fetch handles exceptions this method typically will not throw
   * (but if you wish to be extra cautious it may)
   */
  def fetchUnsafe[T](key: String,
    r: RArgument = RArgument(),
    pr: PRArgument = PRArgument(),
    notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
    returnDeletedVClock: ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(),
    ifModified: IfModifiedVClockArgument = IfModifiedVClockArgument())(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): ValidationNel[Throwable, Option[T]] = {
    fetch(key, r, pr, notFoundOk, basicQuorum, returnDeletedVClock, ifModified).unsafePerformIO()
  }

  // ifNoneMatch - bool - store
  // ifNotModified - bool - store
  def store[T](obj: T,
    r: RArgument = RArgument(),
    pr: PRArgument = PRArgument(),
    notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
    returnDeletedVClock: ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(),
    w: WArgument = WArgument(),
    pw: PWArgument = PWArgument(),
    dw: DWArgument = DWArgument(),
    returnBody: ReturnBodyArgument = ReturnBodyArgument(),
    ifNoneMatch: Boolean = false,
    ifNotModified: Boolean = false)(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T], mutator: ScaliakMutation[T]): IO[ValidationNel[Throwable, Option[T]]] = {

    val obk = converter.write(obj)

    (Validation.fromTryCatch[IO[ValidationNel[Throwable, Option[T]]]] {
      new RiakObject().setValue(BinaryValue.create(obk._bytes))
      for {
        resp <- rawFetch(obk._key, r, pr, notFoundOk, basicQuorum, returnDeletedVClock)
        fetchRes <- riakFetchResponseToResult(obk._key, resp, returnDeletedVClock.allowTombstones).pure[IO]
      } yield {
        fetchRes flatMap {
          mbFetched =>
            {
              //TODO: we don't handle failure here.
              val vclk = resp map { todo => (todo.getObjectList.asScala.headOption getOrElse new RiakObject()).getVClock() }

              val objToStore = converter.write(mutator(mbFetched, obj)).asRiak(name, vclk.get)

              val storeMeta = prepareStoreOps(obk._key, objToStore, w, pw, dw, returnBody, ifNoneMatch, ifNotModified)

              riakStoreResponseToResult(obk._key,
                retrier[Try[StoreOperation.Response]] {
                  runOnClient(_.store(storeMeta))
                },
                returnDeletedVClock.allowTombstones)
            }
        }
      }
    } leftMap { t: Throwable => t }).disjunction.valueOr(err => err.failureNel[Option[T]].pure[IO])
  }

  /*
  * This should only be used in cases where the consequences are understood.
  * With a bucket that has allow_mult set to true, using "put" instead of "store"
  * will result in significantly more conflicts
  */
  def put[T](obj: T,
    w: WArgument = WArgument(),
    pw: PWArgument = PWArgument(),
    dw: DWArgument = DWArgument(),
    returnBody: ReturnBodyArgument = ReturnBodyArgument())(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): IO[ValidationNel[Throwable, Option[T]]] = {
    val wobj = converter.write(obj)
    retrier[IO[Try[StoreOperation.Response]]] {
      val objToStore = wobj.asRiak(name, null)
      runOnClient {
        _.store(prepareStoreOps(wobj._key, objToStore, w, pw, dw, returnBody)).pure[IO]
      }
    } map {
      riakStoreResponseToResult(wobj._key, _)
    } except {
      _.failureNel[Option[T]].pure[IO]
    }

  }

  // r - int
  // pr - int
  // w - int
  // dw - int
  // pw - int
  // rw - int
  def delete[T](obj: T, fetchBefore: Boolean = false)(implicit converter: ScaliakConverter[T]): IO[Validation[Throwable, Unit]] = {
    deleteByKey(converter.write(obj)._key, fetchBefore)
  }

  def deleteByKey(key: String, fetchBefore: Boolean = false): IO[Validation[Throwable, Unit]] = {
    val deleteMetaBuilder = new DeleteOperation.Builder(new Location(ns, key));
    val emptyFetchMeta = new FetchOperation.Builder(new Location(ns, key)).build()

    val mbFetchHead = (fetchBefore.some map { x =>
      runOnClient {
        _.fetch(emptyFetchMeta).pure[IO]
      }
    }).get 

    (for {
      mbHeadResponse <- mbFetchHead
      deleteMeta <- retrier[IO[DeleteOperation]](prepareDeleteMeta(mbHeadResponse.toOption, deleteMetaBuilder).pure[IO])
      _ <- {
        runOnClient {
          _.delete(deleteMeta).pure[IO]
        }
      }
    } yield (println("[delete] success for key" + key)).success[Throwable]) except {
      t => t.failure[Unit].pure[IO]
    }
  }

  def fetchIndexByValue(index: String, value: String): IO[Validation[Throwable, List[String]]] = {
    val biq = new BinIndexQuery.Builder(ns, index, value)
      .withKeyAndIndex(true)
      .withMaxResults(Integer.MAX_VALUE)
      .withContinuation(BinaryValue.create("continuation"))
      .withPaginationSort(true)
      .withRegexTermFilter("filter")
      .build();

    val query = new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((index + "_bin").getBytes()))
      /*      .withRangeStart(BinaryValue.unsafeCreate("foo00".getBytes()))
            .withRangeEnd(BinaryValue.unsafeCreate("foo19".getBytes()))
            .withRegexTermFilter(BinaryValue.unsafeCreate("2".getBytes()))*/
      .withIndexKey(BinaryValue.unsafeCreate(value.getBytes))
      .withReturnKeyAndIndex(true)
      .withPaginationSort(true)
      .build();

    fetchValueIndex(new SecondaryIndexQueryOperation.Builder(query)
      .build())

  }

  def fetchIndexByValue(index: String, value: Int): IO[Validation[Throwable, List[String]]] = {
    val query =
      new SecondaryIndexQueryOperation.Query.Builder(ns, BinaryValue.unsafeCreate((index + "_int").getBytes()))
        .withIndexKey(BinaryValue.unsafeCreate(String.valueOf(value).getBytes()))
        .build()

    fetchValueIndex(new SecondaryIndexQueryOperation.Builder(query)
      .build())
  }

  private def fetchValueIndex(query: SecondaryIndexQueryOperation): IO[Validation[Throwable, List[String]]] = {
    runOnClient {
      _.fetchIndex(query).pure[IO].map { todo => todo.get.success[Throwable] }.except {
        _.failure[List[String]].pure[IO]
      }

    }
  }

  private def rawFetch(key: String,
    r: RArgument,
    pr: PRArgument,
    notFoundOk: NotFoundOkArgument,
    basicQuorum: BasicQuorumArgument,
    returnDeletedVClock: ReturnDeletedVCLockArgument,
    ifModified: IfModifiedVClockArgument = IfModifiedVClockArgument()) = {
    val fetchMetaBuilder = new FetchOperation.Builder(new Location(ns, key))
    List(r, pr, notFoundOk, basicQuorum, returnDeletedVClock, ifModified) foreach { _ addToMeta fetchMetaBuilder }

    retrier[IO[Try[FetchOperation.Response]]] {
      runOnClient(_.fetch(fetchMetaBuilder.build).pure[IO])
    }
  }

  private def riakFetchResponseToResult[T](key: String, r: Try[FetchOperation.Response], allowTombstones: Boolean = false)(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): ValidationNel[Throwable, Option[T]] = {
    r.map { todo =>
      ((todo.getObjectList.asScala filter {
        allowTombstones || !_.isDeleted
      } map { x =>
        {
          val rro: ReadObject = new RichRiakObject(key, name, x)
          converter.read(rro)
        }
      }).toList.toNel map { sibs =>
        resolver(sibs)
      }).sequence[ScaliakConverter[T]#ReadResult, T]
    } getOrElse (new RuntimeException("::").failureNel)

  }

  private def riakStoreResponseToResult[T](key: String, r: Try[StoreOperation.Response], allowTombstones: Boolean = false)(implicit converter: ScaliakConverter[T], resolver: ScaliakResolver[T]): ValidationNel[Throwable, Option[T]] = {
    r.map { todo =>
      ((todo.getObjectList.asScala filter {
        allowTombstones || !_.isDeleted
      } map { x =>
        {
          val rro: ReadObject = new RichRiakObject(key, name, x)
          converter.read(rro)
        }
      }).toList.toNel map { sibs =>
        resolver(sibs)
      }).sequence[ScaliakConverter[T]#ReadResult, T]
    } getOrElse (new RuntimeException("::").failureNel)
  }

  private def prepareStoreOps(key: String, obj: RiakObject, w: WArgument, pw: PWArgument, dw: DWArgument,
    returnBody: ReturnBodyArgument,
    ifNoneMatch: Boolean = false,
    ifNotModified: Boolean = false) = {
    val storeMetaBuilder = new StoreOperation.Builder(new Location(ns, key))
    List(w, pw, dw, returnBody) foreach { _ addToMeta storeMetaBuilder }
    if (ifNoneMatch) storeMetaBuilder.withIfNoneMatch(ifNoneMatch)
    if (ifNotModified) storeMetaBuilder.withIfNotModified(ifNotModified)
    storeMetaBuilder.withContent(obj)
    storeMetaBuilder.build
  }

  private def prepareDeleteMeta(mbResponse: Option[FetchOperation.Response], deleteMetaBuilder: DeleteOperation.Builder): DeleteOperation = {
    val mbPrepared = for {
      response <- mbResponse
      vClock <- Option(response.getObjectList.get(0).getVClock)
    } yield deleteMetaBuilder.withVclock(vClock)
    (mbPrepared | deleteMetaBuilder).build
  }

  /*
   * TO-DO
   
    private def generateLinkWalkSpec(bucket: String, key: String, steps: LinkWalkSteps) = {
    new LinkWalkSpec(steps, bucket, key)
  }

  private def generateMapReduceSpec(mapReduceJSONString: String) = {
    new MapReduceSpec(mapReduceJSONString)
  }

   
  import linkwalk._

  // This method discards any objects that have conversion errors
  def linkWalk[T](obj: ReadObject, steps: LinkWalkSteps)(implicit converter: ScaliakConverter[T]): IO[Iterable[Iterable[T]]] = {
    for {
      walkResult <- {
        runOnClient(_.linkWalk(generateLinkWalkSpec(name, obj.key, steps)).pure[IO])
      }
    } yield {
      walkResult.asScala map {
        _.asScala flatMap {
          converter.read(_).toOption
        }
      } filterNot {
        _.isEmpty
      }
    }
  }

  def mapReduce(job: MapReduceJob): IO[Validation[Throwable, MapReduceResult]] = {
    val jobAsJSON = mapreduce.MapReduceBuilder.toJSON(job)
    val spec = generateMapReduceSpec(jobAsJSON.toString)
    retrier {
      runOnClient(_.mapReduce(spec).pure[IO])
    }.map(_.success[Throwable]).except {
      _.failure[MapReduceResult].pure[IO]
    }
  }

  def mapReduce[T, U, A](job: MapReduceJob, theClass: Class[T], converter: T => U, iter: IterV[U, A]): IO[Validation[Throwable, IterV[U,A]]] = {
    val jobAsJSON = mapreduce.MapReduceBuilder.toJSON(job)
    val spec = generateMapReduceSpec(jobAsJSON.toString)
    retrier {
      runOnClient(_.mapReduce(spec, theClass, converter, iter))
    }.map(_.success[Throwable]).except {
      _.failure[IterV[U, A]].pure[IO]
    }
  }
  *
  */

  private def retrier[R](f: => R, attempts: Int = 3): R = {
    try {
      f
    } catch {
      case e: Throwable => {
        if (attempts == 0) {
          throw e
        } else {
          retrier(f, attempts - 1)
        }
      }
    }
  }
}

trait ScaliakConverter[T] {
  type ReadResult[T] = ValidationNel[Throwable, T]

  def read(o: ReadObject): ReadResult[T]

  def write(o: T): WriteObject
}

object ScaliakConverter extends ScaliakConverters {
  implicit lazy val DefaultConverter = PassThroughConverter
}

trait ScaliakConverters {

  def newConverter[T](r: ReadObject => ValidationNel[Throwable, T], w: T => WriteObject) = new ScaliakConverter[T] {

    def read(o: ReadObject) = r(o)

    def write(o: T) = w(o)
  }

  lazy val PassThroughConverter = newConverter[ReadObject](
    ((o: ReadObject) =>
      o.successNel[Throwable]),
    ((o: ReadObject) =>
      WriteObject(key = o.key, value = o.bytes, contentType = o.contentType,
        links = o.links, metadata = o.metadata, binIndexes = o.binIndexes, intIndexes = o.intIndexes,
        vTag = o.vTag, lastModified = o.lastModified)))
}

sealed trait ScaliakResolver[T] {

  def apply(siblings: NonEmptyList[ValidationNel[Throwable, T]]): ValidationNel[Throwable, T]

}

object ScaliakResolver extends ScaliakResolvers {

  implicit def DefaultResolver[T] = newResolver[T](
    siblings =>
      if (siblings.count == 1) siblings.head
      else throw new UnresolvedConflictException(null, "there were siblings", new java.util.ArrayList()))

}

trait ScaliakResolvers {
  def newResolver[T](resolve: NonEmptyList[ValidationNel[Throwable, T]] => ValidationNel[Throwable, T]) = new ScaliakResolver[T] {
    def apply(siblings: NonEmptyList[ValidationNel[Throwable, T]]) = resolve(siblings)
  }
}

trait ScaliakMutation[T] {

  def apply(storedObject: Option[T], newObject: T): T

}

object ScaliakMutation extends ScaliakMutators {
  implicit def DefaultMutation[T] = ClobberMutation[T]
}

trait ScaliakMutators {

  def newMutation[T](mutate: (Option[T], T) => T) = new ScaliakMutation[T] {
    def apply(o: Option[T], n: T) = mutate(o, n)
  }

  def ClobberMutation[T] = newMutation((o: Option[T], n: T) => n)

}

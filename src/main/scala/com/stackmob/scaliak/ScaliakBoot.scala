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
package com.stackmob.scaliak

import scalaz._
import Scalaz._
import scalaz.effect.IO
import java.io.IOException
import com.basho.riak.client.api.RiakException
import com.basho.riak.client.core.operations.StoreBucketPropsOperation
import com.basho.riak.client.core.operations.FetchBucketPropsOperation
import com.basho.riak.client.core.query.BucketProperties
import com.basho.riak.client.core.query.Namespace
import scala.util.Try
/**
 * @author ram
 *
 */
trait ScaliakBoot {

  def rawOrPool: scala.util.Either[RawClientWithStreaming, ScaliakClientPool]

  def runOnClient[A](f: RawClientWithStreaming => A): A = {
    rawOrPool match {
      case Left(client) => f(client)
      case Right(pool)  => pool.withClient[A](f)
    }
  }

  def listBuckets: IO[Validation[Throwable, Set[String]]] =
    //TO-DO : todo.{map _}.get is actually Try.get. We need to handle exception.
    runOnClient(_.listBuckets()).pure[IO].map { todo => { todo.map { z: List[String] => z.toSet }.get }.success[Throwable] }.except {
      _.failure[Set[String]].pure[IO]
    }

  def bucket(name: String,
    allowSiblings: AllowSiblingsArgument = AllowSiblingsArgument(),
    lastWriteWins: LastWriteWinsArgument = LastWriteWinsArgument(),
    nVal: NValArgument = NValArgument(),
    r: RArgument = RArgument(),
    w: WArgument = WArgument(),
    rw: RWArgument = RWArgument(),
    dw: DWArgument = DWArgument(),
    pr: PRArgument = PRArgument(),
    pw: PWArgument = PWArgument(),
    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
    notFoundOk: NotFoundOkArgument = NotFoundOkArgument()): IO[Validation[Throwable, ScaliakBucket]] = {
    val metaArgs = List(allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk)

    val isUpdateBucket = metaArgs.exists(_.value.isDefined) // update if more one or more arguments is passed in

    val ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, name)

    val fetchPropsBuilder = new FetchBucketPropsOperation.Builder(ns)

    val fetchAction = runOnClient(_.fetchBucket(fetchPropsBuilder.build()))

    val storeAction = (if (isUpdateBucket) {
      runOnClient(_.storeBucket(createUpdateBucketProps(name, allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk))).flatMap(_ => fetchAction)
    } else Try(null))

    (for {
      sbp <- storeAction
      fbp <- fetchAction
      //TO-DO : todo.get is actually Try.get.We need to handle exception.
    } yield buildBucket(fbp, name)).pure[IO].map { todo => todo.get.success[Throwable] }.except {
      _.failure[ScaliakBucket].pure[IO]
    }

  }

  private def buildBucket(b: BucketProperties, name: String) = {
    val precommits = Option(b.getPrecommitHooks).cata(_.toArray.toSeq, Nil) map {
      _.asInstanceOf[com.basho.riak.client.core.query.functions.Function]
    }
    val postcommits = Option(b.getPostcommitHooks).cata(_.toArray.toSeq, Nil) map {
      _.asInstanceOf[com.basho.riak.client.core.query.functions.Function]
    }
    new ScaliakBucket(
      rawClientOrClientPool = rawOrPool,
      name = name,
      allowSiblings = b.getAllowMulti,
      lastWriteWins = b.getLastWriteWins,
      nVal = b.getNVal,
      backend = Option(b.getBackend),
      smallVClock = b.getSmallVClock,
      bigVClock = b.getBigVClock,
      youngVClock = b.getYoungVClock,
      oldVClock = b.getOldVClock,
      precommitHooks = precommits,
      postcommitHooks = postcommits,
      rVal = b.getR,
      wVal = b.getW,
      rwVal = b.getRw,
      dwVal = b.getDw,
      prVal = b.getPr,
      pwVal = b.getPw,
      basicQuorum = b.getBasicQuorum,
      notFoundOk = b.getNotFoundOk,
      chashKeyFunction = b.getChashKeyFunction,
      linkWalkFunction = b.getLinkwalkFunction,
      isSearchable = b.getSearchIndex)
  }

  private def createUpdateBucketProps(name: String, allowSiblings: AllowSiblingsArgument,
    lastWriteWins: LastWriteWinsArgument,
    nVal: NValArgument,
    r: RArgument,
    w: WArgument,
    rw: RWArgument,
    dw: DWArgument,
    pr: PRArgument,
    pw: PWArgument,
    basicQuorum: BasicQuorumArgument,
    notFoundOk: NotFoundOkArgument) = {
    val ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, name)

    val builder = new StoreBucketPropsOperation.Builder(ns)
    val alList = List(allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk)
    alList.foreach { _ addToMeta builder }
    builder.build
  }

}
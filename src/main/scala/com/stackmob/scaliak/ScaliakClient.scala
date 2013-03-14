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
import effects._

import com.basho.riak.client.raw.RawClient
import java.io.IOException
import com.basho.riak.client.http.response.RiakIORuntimeException
import com.basho.riak.client.query.functions.{ NamedErlangFunction, NamedFunction }
import scala.collection.JavaConversions._
import com.basho.riak.client.bucket.BucketProperties
import com.basho.riak.client.builders.BucketPropertiesBuilder

class ScaliakClient(rawClient: RawClientWithStreaming, secHTTPClient: Option[RawClient] = None) {

  private val bucketPropertyClient = secHTTPClient getOrElse rawClient

  def listBuckets: IO[Set[String]] = {
    rawClient.listBuckets().pure[IO] map { _.toSet }
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

    val updateBucket = (metaArgs map { _.value.isDefined }).asMA.sum // update if more one or more arguments is passed in

    val fetchAction = bucketPropertyClient.fetchBucket(name).pure[IO]

    val fullAction = if (updateBucket) {
      bucketPropertyClient.updateBucket(name,
        createUpdateBucketProps(allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk)
      ).pure[IO] >>=| fetchAction
    } else {
      fetchAction
    }

    (for {
      b <- fullAction
    } yield buildBucket(b, name)) catchSomeLeft { (t: Throwable) =>
      t match {
        case t: IOException            => t.some
        case t: RiakIORuntimeException => t.getCause.some
        case _                         => none
      }
    } map { _ match {
      case Left(e) => e.fail
      case Right(s) => s.success
    }}
  }

  // this method causes side effects and may throw
  // exceptions with the PBCAdapter
  def clientId = Option {
    bucketPropertyClient.getClientId
  }

  def setClientId(id: Array[Byte]) = {
    bucketPropertyClient.setClientId(id)
    this
  }

  def generateAndSetClientId(): Array[Byte] = {
    bucketPropertyClient.generateAndSetClientId()
  }

  def shutdown() {
    secHTTPClient.foreach(_.shutdown())
    rawClient.shutdown()
  }
  
  def ping() { rawClient.ping() }

  private def buildBucket(b: BucketProperties, name: String) = {
    val precommits = Option(b.getPrecommitHooks).cata(_.toArray.toSeq, Nil) map { _.asInstanceOf[NamedFunction] }
    val postcommits = Option(b.getPostcommitHooks).cata(_.toArray.toSeq, Nil) map { _.asInstanceOf[NamedErlangFunction] }
    new ScaliakBucket(
      rawClientOrClientPool = Left(rawClient),
      name = name,
      allowSiblings = b.getAllowSiblings,
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
      rwVal = b.getRW,
      dwVal = b.getDW,
      prVal = b.getPR,
      pwVal = b.getPW,
      basicQuorum = b.getBasicQuorum,
      notFoundOk = b.getNotFoundOK,
      chashKeyFunction = b.getChashKeyFunction,
      linkWalkFunction = b.getLinkWalkFunction,
      isSearchable = b.getSearch
    )
  }

  private def createUpdateBucketProps(allowSiblings: AllowSiblingsArgument,
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
    val builder = new BucketPropertiesBuilder
    val alList = List(allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk)
    alList.foreach { _ addToMeta builder }
    builder.build
  }

}

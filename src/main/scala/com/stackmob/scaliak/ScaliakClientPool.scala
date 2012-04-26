package com.stackmob.scaliak

import scalaz._
import Scalaz._
import effects._

import org.apache.commons.pool._
import org.apache.commons.pool.impl._

import java.io.IOException

import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.raw.pbc.PBClientAdapter
import com.basho.riak.client.raw.RawClient

import com.basho.riak.client.http.response.RiakIORuntimeException
import com.basho.riak.client.query.functions.{ NamedErlangFunction, NamedFunction }
import scala.collection.JavaConversions._
import com.basho.riak.client.bucket.BucketProperties
import com.basho.riak.client.raw.{ Transport ⇒ RiakTransport }
import com.basho.riak.client.builders.BucketPropertiesBuilder


abstract class ScaliakClientPool {
  def withClient[T](body: RawClient => T): T
}

private class ScaliakPbClientFactory(host: String, port: Int) extends PoolableObjectFactory {
  def makeObject = new PBClientAdapter(host, port) 

  def destroyObject(sc: Object): Unit = { 
	  sc.asInstanceOf[RawClient].shutdown
	  // Methods for client destroying
  }

  def passivateObject(sc: Object): Unit = {} 
  def validateObject(sc: Object) = {
    try {
      //sc.asInstanceOf[RawClient].ping
      true
    }
    catch {
      case e => false 
    }
  }

  def activateObject(sc: Object): Unit = {}
}

class ScaliakPbClientPool(host: String, port: Int, httpPort: Int) extends ScaliakClientPool {
  val pool = new StackObjectPool(new ScaliakPbClientFactory(host, port))
  val secHTTPClient = new HTTPClientAdapter("http://" + host + ":" + httpPort + "/riak")
  
  override def toString = host + ":" + String.valueOf(port)

  def withClient[T](body: RawClient => T): T = {
    val client = pool.borrowObject.asInstanceOf[RawClient]
    val response = body(client) 
    pool.returnObject(client)
    response
  }

  // close pool & free resources
  def close = pool.close
  
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

    val fetchAction = secHTTPClient.fetchBucket(name).pure[IO]
    val fullAction = if (updateBucket) {
      secHTTPClient.updateBucket(name,
        createUpdateBucketProps(allowSiblings, lastWriteWins, nVal, r, w, rw, dw, pr, pw, basicQuorum, notFoundOk)
      ).pure[IO] >>=| fetchAction
    } else {
      fetchAction
    }

    (for {
      b ← fullAction
    } yield buildBucket(b, name)) catchSomeLeft { (t: Throwable) ⇒
      t match {
        case t: IOException            ⇒ t.some
        case t: RiakIORuntimeException ⇒ t.getCause.some
        case _                         ⇒ none
      }
    } map { validation(_) }
  }
  
  private def buildBucket(b: BucketProperties, name: String) = {
    val precommits = Option(b.getPrecommitHooks).cata(_.toArray.toSeq, Nil) map { _.asInstanceOf[NamedFunction] }
    val postcommits = Option(b.getPostcommitHooks).cata(_.toArray.toSeq, Nil) map { _.asInstanceOf[NamedErlangFunction] }
    new ScaliakBucket(
      rawClientOrClientPool = Right(this),
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
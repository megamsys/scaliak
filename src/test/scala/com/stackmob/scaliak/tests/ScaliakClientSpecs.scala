package com.stackmob.scaliak.tests

import org.specs2._
import mock._
import scalaz._
import Scalaz._
import scalaz.effects._
import com.stackmob.scaliak.{RawClientWithStreaming, ScaliakClient}
import java.io.IOException
import com.basho.riak.client.raw.http.HTTPClientAdapter
import java.util.LinkedList
import com.basho.riak.client.query.functions.{NamedFunction, NamedErlangFunction}
import com.basho.riak.client.cap.Quorum
import com.basho.riak.client.RiakException
import util.MockitoArgumentExtractor
import com.basho.riak.client.bucket.BucketProperties
import org.mockito.{Matchers => MM}
import com.basho.riak.client.raw.Transport

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/8/11
 * Time: 8:44 PM
*/

class ScaliakClientSpecs extends Specification with Mockito { def is = args(sequential=true) ^ // mocks aren't thread safe
  "Scaliak Client".title                                                            ^
  """
  The Scalaik Client wraps the low-level RiakClient to provide
  a "scala-ified" API similar to the high-level riak client.


  This class provides similar functionality to the IRiakClient
  interface
  """                                                                               ^
                                                                                    p^
  "Working With Buckets"                                                            ^
    "Retrieving a bucket as is or with defaults"                                    ^
      "if the IO action succeeds"                                                   ^
        "returns a success containing the bucket given a bucket name"               ! fetchSuccess ^
        "the returned bucket contains the bucket name"                              ! { bucket must beSome.which { _.name == bucketName } } ^
        "the returned bucket has the correct allow siblings value"                  ! allowSibsSet ^
        "the returned bucket has the correct last write wins value"                 ! { bucket must beSome.which { _.lastWriteWins == lastWrite } } ^
        "the returned bucket has the correct nval"                                  ! { bucket must beSome.which { _.nVal == nVal } } ^
        "the returned bucket's backend is None if it is null"                       ! nullBackend ^
        "the returned bucket's backed is set to Some if otherwise"                  ! setBackend ^
        "the returned bucket has the correct small vclock"                          ! { bucket must beSome.which { _.smallVClock == smallVC } } ^
        "the returned bucket has the correct big vclock"                            ! { bucket must beSome.which { _.bigVClock == bigVC } } ^
        "the returned bucket has the correct young vclock"                          ! { bucket must beSome.which { _.youngVClock == youngVC }} ^
        "the returned bucket has the correct old vclock"                            ! { bucket must beSome.which { _.oldVClock == oldVC } } ^
        "the returned bucket's precommit hooks are empty if null is returned"       ! nullPrecommit ^
        "the returned bucket's precommit hooks are a Seq mathcing the collection"   ! existingPrecommits ^
        "the returned bucket's postcommit hooks are empty if null is returned"      ! nullPostcommit ^
        "the returned bucket's postcommit hooks are a Seq matching the collection"  ! existingPostcommits ^
        "the returned bucket has the correct r value"                               ! { bucket must beSome.which(_.rVal == rVal) } ^
        "the returned bucket has the correct w value"                               ! { bucket must beSome.which(_.wVal == wVal) } ^
        "the returned bucket has the correct rw value"                              ! { bucket must beSome.which(_.rwVal == rwVal) } ^
        "the returned bucket has the correct dw value"                              ! { bucket must beSome.which(_.dwVal == dwVal) } ^
        "the returned bucket has the correct pr value"                              ! { bucket must beSome.which(_.prVal == prVal) } ^
        "the returned bucket has the correct pw value"                              ! { bucket must beSome.which(_.pwVal == pwVal) } ^
        "the returned bucket has the correct basic quorum value"                    ! { bucket must beSome.which(_.basicQuorum == basicQuorum) } ^
        "the returned bucket has the correct get not found ok value"                ! { bucket must beSome.which(_.notFoundOk == notFoundOK) } ^
        "the returned bucket has the correct chashkey function"                     ! { bucket must beSome.which(_.chashKeyFunction == chashFun) } ^
        "the returned bucket has the correct link walk function"                    ! { bucket must beSome.which(_.linkWalkFunction == linkWalkFun) } ^
        "the returned bucket has the correct search value"                          ! { bucket must beSome.which(_.isSearchable == searchVal) } ^                                                                            p^
      "if the IO action throws"                                                     ^
        "returns a failure containing the exception"                                ! fetchFailure ^
                                                                                    p^p^
    "Creating/Updating Buckets"                                                     ^
       "if all IO actions succeed"                                                  ^
         "updates the bucket building the meta props from the args passed in"       ! takesMetaProps ^
         "returns the updated bucket"                                               ! returnsOnUpdate ^p^
       "if the fetch throws an exception"                                           ^
         "returns a failure containing the exception"                               ! failsOnFetchException ^p^
       "if the update throws an exception"                                          ^
         "returns a failure returns a failure containing the exception"             ! failsOnUpdateException ^
                                                                                    p^p^
    "Listing Buckets"                                                               ^
      "returns an empty set if the raw client returns empty set"                    ! listBucketsEmpty ^
      "returns a Scala Set of the Java Set returned by raw client"                  ! listBucketsNonEmpty ^
      "does not setup exception handling for the IO action"                         ! listBucketsException ^
                                                                                    endp^
  "Working with Client IDs"                                                         ^
    "Setting the Client ID By Value"                                                ^
      "sets the value using the raw client"                                         ! setClientId ^
      "returns the client whose id is being set"                                    ! setClientIdReturnsSelf ^p^
    "Setting the Client ID Randomly"                                                ^      
      "returns the generated value"                                                 ! autoGenId ^p^
    "Getting the Client ID"                                                         ^
      "returns a some containing the client id if one exists"                       ! getExistingClientId ^
      "returns a none if a client id has not been set"                              ! getMissingClientId ^
                                                                                    endp^
  "Pinging Riak"                                                                    ^
    "returns true if no exception is thrown by the raw client"                      ! skipped ^
    "returns false if an exception is thrown by the raw client"                     ! skipped ^
                                                                                    end


  val rawClient = mock[RawClientWithStreaming]
  val client = new ScaliakClient(rawClient)

  val bucketName = "test"
  val bucketProps = mock[com.basho.riak.client.bucket.BucketProperties]

  rawClient.fetchBucket(bucketName) returns bucketProps

  val nVal = 1; bucketProps.getNVal returns nVal
  val lastWrite = false; bucketProps.getLastWriteWins returns lastWrite
  val smallVC = 1; bucketProps.getSmallVClock returns smallVC
  val bigVC = 2; bucketProps.getBigVClock returns bigVC
  val youngVC = 3; bucketProps.getYoungVClock returns youngVC
  val oldVC = 4; bucketProps.getOldVClock returns oldVC
  val rVal = new Quorum(1); bucketProps.getR returns rVal
  val wVal = new Quorum(2); bucketProps.getW returns wVal
  val rwVal = new Quorum(3); bucketProps.getRW returns rwVal
  val dwVal = new Quorum(4); bucketProps.getDW returns dwVal
  val prVal = new Quorum(5); bucketProps.getPR returns prVal
  val pwVal = new Quorum(6); bucketProps.getPW returns pwVal
  val basicQuorum = true; bucketProps.getBasicQuorum returns basicQuorum
  val notFoundOK = false; bucketProps.getNotFoundOK returns notFoundOK
  val chashFun = mock[NamedErlangFunction]; bucketProps.getChashKeyFunction returns chashFun
  val linkWalkFun = mock[NamedErlangFunction]; bucketProps.getLinkWalkFunction returns linkWalkFun
  val searchVal = false; bucketProps.getSearch returns searchVal

  lazy val bucket = client.bucket(bucketName).unsafePerformIO.toOption

  def fetchSuccess = {
    val mbBucket = client.bucket(bucketName).unsafePerformIO.toOption
    mbBucket must beSome
  }

  def allowSibsSet = {
    bucketProps.getAllowSiblings returns true
    val trueSibs = (client.bucket(bucketName).unsafePerformIO.toOption map { _.allowSiblings }) | false
    bucketProps.getAllowSiblings returns false
    val falseSibs = (client.bucket(bucketName).unsafePerformIO.toOption map { _.allowSiblings }) | true

    (trueSibs, falseSibs) must beEqualTo(true, false)
  }

  def nullBackend = {
    bucketProps.getBackend returns null
    client.bucket(bucketName).unsafePerformIO.toOption must beSome.like {
      case b => b.backend must beNone
    }
  }

  def setBackend = {
    bucketProps.getBackend returns "some_backend"
    client.bucket(bucketName).unsafePerformIO.toOption must beSome.like {
      case b => b.backend must beSome.which { _ == "some_backend" }
    }
  }

  def nullPrecommit = {
    bucketProps.getPrecommitHooks returns null
    client.bucket(bucketName).unsafePerformIO.toOption must beSome.like {
      case b => b.precommitHooks must beEmpty
    }
  }

  def existingPrecommits = {
    val dummy = new java.util.LinkedList[NamedFunction]
    dummy.add(mock[NamedFunction])
    dummy.add(mock[NamedFunction])
    bucketProps.getPrecommitHooks returns dummy.asInstanceOf[java.util.Collection[NamedFunction]]
    client.bucket(bucketName).unsafePerformIO.toOption must beSome.like {
      case b => b.precommitHooks must haveSize(2)
    }
  }

  def nullPostcommit = {
    bucketProps.getPostcommitHooks returns null
    client.bucket(bucketName).unsafePerformIO.toOption must beSome.like {
      case b => b.postcommitHooks must beEmpty
    }
  }

  def existingPostcommits = {
    val dummy = new java.util.LinkedList[NamedErlangFunction]
    dummy.add(mock[NamedErlangFunction])
    bucketProps.getPostcommitHooks returns dummy.asInstanceOf[java.util.Collection[NamedErlangFunction]]
    client.bucket(bucketName).unsafePerformIO.toOption must beSome.like {
      case b => b.postcommitHooks must haveSize(1)
    }
  }

  def fetchFailure = {
    rawClient.fetchBucket(bucketName) throws (new IOException) thenReturns bucketProps // put the mock back in its original state after throwing
    val notBucket = client.bucket(bucketName).unsafePerformIO.either
    notBucket must beLeft
  }

  def listBucketsEmpty = {
    rawClient.listBuckets() returns (new java.util.HashSet[String]())
    client.listBuckets.unsafePerformIO must beEmpty
  }
  
  def listBucketsNonEmpty = {
    val s = new java.util.HashSet[String]()
    s.add("bucketname")
    s.add("bucketname2")
    rawClient.listBuckets() returns s
    
    client.listBuckets.unsafePerformIO must haveTheSameElementsAs("bucketname" :: "bucketname2" :: Nil)
  }
  
  def listBucketsException = {
    rawClient.listBuckets() throws new NullPointerException
    client.listBuckets.unsafePerformIO must throwA[NullPointerException]
  }

  def takesMetaProps = {
    // this test is crap but cant write an extractor for an argument for a function we cant stub
    client.bucket(bucketName, nVal = 2, r = 2, w = 2, rw = 3, dw = 3).unsafePerformIO

    there was one(rawClient).updateBucket(MM.eq(bucketName), MM.isA(classOf[BucketProperties]))
  }

  def returnsOnUpdate = {
    val r = client.bucket(bucketName, nVal = 2, r = 2, w = 2, rw = 3, dw = 3).unsafePerformIO

    r.toOption must beSome
  }


  def failsOnFetchException = {
    rawClient.fetchBucket(bucketName) throws (new IOException) thenReturns bucketProps // put the mock back in its original state after

    val r = client.bucket(bucketName, nVal = 2, r = 2, w = 2, rw = 3, dw = 3).unsafePerformIO
    r.either must beLeft
  }

  def failsOnUpdateException = {
    rawClient.updateBucket(MM.eq(bucketName), MM.isA(classOf[BucketProperties])) throws (new IOException)

    val r = client.bucket(bucketName, nVal = 2, r = 2, w = 2, rw = 3, dw = 3).unsafePerformIO
    r.either must beLeft
  }

  def setClientId = {
    val id = "1234".getBytes
    client.setClientId(id)

    there was one(rawClient).setClientId(MM.eq("1234".getBytes))
  }

  def setClientIdReturnsSelf = {
    client.setClientId("1234".getBytes) must beEqualTo(client)
  }
  
  def autoGenId = {
    client.generateAndSetClientId()
    there was one(rawClient).generateAndSetClientId()
  }

  def getExistingClientId = {
    rawClient.getClientId() returns "1234".getBytes
    client.clientId must beSome.like {
      case bytes => new String(bytes) must_== "1234"
    }
  }

  def getMissingClientId = {
    rawClient.getClientId returns null
    client.clientId must beNone
  }
}
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

package com.stackmob.scaliak.tests

import org.specs2._
import mock._
import scalaz._
import Scalaz._
import com.basho.riak.client.query.LinkWalkStep.Accumulate
import com.stackmob.scaliak._
import linkwalk._
import com.basho.riak.client.raw.RawClient
import com.basho.riak.client.query.functions.NamedErlangFunction
import com.basho.riak.client.raw.query.LinkWalkSpec
import org.mockito.{Matchers => MM}
import com.basho.riak.client.query.{WalkResult, LinkWalkStep => JLinkWalkStep}
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.cap.{UnresolvedConflictException, Quorum, VClock}

class LinkWalkingSpecs extends Specification with Mockito with util.MockRiakUtils { def is =
  "Link Walking DSL".title                                                          ^
  """
  Scaliak provides a simple DSL for link walkink.
  The DSL expresses the start of the link walk (a bucket, object pair)
  and the steps of the link walk. Like the rest of Scaliak all
  link walk actions are wrapped in an IO to be execute by the underlying
  raw client from the bucket
  """                                                                               ^
                                                                                    p^
  "Link Walk Steps"                                                                 ^
    "Implicit Conversion to Java LinkWalkStep"                                      ^
      "Has correct starting bucket"                                                 ! javaImplicitCorrectBucket ^
      "Has correct starting tag"                                                    ! javaImplicitCorrectTag ^
      "If instantiated with shouldAccumulate = true then has Accumulate.YES"        ! javaImplicitAccumulateYes ^
      "If instantiated with shouldAccumulate = false then has Accumulate.NO"        ! javaImplicitAccumulateNo ^
      "If instantiated without shouldAccumulate then has Accumulate.DEFAULT"        ! javaImplicitAccumulateDefault ^
                                                                                    p^
    "Implicit Conversion from (String, String, Boolean)"                            ^
      "instantiates the step correctly"                                             ! tuple3implicit ^
      "can also call .toLinkWalkStep explicitly"                                    ! tuple3explicit ^
                                                                                    p^    
    "Implicit Conversion from (String, String)"                                     ^
      "instantiates the step correctly leaving shouldAccumulate to the default"     ! tuple2implicit ^
      "can also call .toLinkWalkStep explicitly"                                    ! tuple2explicit ^
                                                                                    p^
    "Multiplying By N"                                                              ^
      "Returns a NonEmptyList with N elements repeated"                             ! testMultiply ^
      "multiply by number less than 1 returns nel(this)"                            ! testMultiplyLessThan1 ^
      "With implicit conversion"                                                    ! testMultiplyImplicit ^
      "can multiply a list of steps (e.g. making 2 become 4)"                       ! testMultiplyList ^
                                                                                    p^
    "Sequencing Steps"                                                              ^
      "adding two instances together contains a nel with left operand as the head"  ! testAdd2 ^
      "can add together multiple elements adding to the list in order"              ! testAdd4 ^
      "can mix with multiplication"                                                 ! testAddMultMix ^
      "mixing with multiplaction using operator syntax preserves op precedence"     ! testAddMultOperatorMix ^
                                                                                    endp^
  "Walking from a Bucket & ReadObject"                                           ^
    "Performs a linkWalk on the raw client passing in the correct start bucket"     ! walkingSimpleObject.testStartBucket ^
    "Passes in the correct start key"                                               ! walkingSimpleObject.testStartKey ^
    "passes in the correct steps"                                                   ! walkingSimpleObject.testStartSteps ^
    "returns an Iterable[Iterable[ReadObject]] of the results"                   ! walkingSimpleObject.testSimpleResults ^
                                                                                    endp^
  "Walking from a Bucket & DomainObject"                                            ^
    "converts all results to domain objects discarding conversion errors"           ! walkingSimpleObject.testDomainResults ^
                                                                                    end
  val lwsBucket = "bucket"
  val lwsTag = "tag"
  val lws = LinkWalkStep(lwsBucket, lwsTag, true)
  val jlws: JLinkWalkStep = linkWalkStepToJLinkWalkStep(lws)

  def javaImplicitCorrectBucket = {
    jlws.getBucket must_== lwsBucket
  }

  def javaImplicitCorrectTag = {
    jlws.getTag must_== lwsTag
  }

  def javaImplicitAccumulateYes = {
    jlws.getKeep must_== Accumulate.YES
  }

  def javaImplicitAccumulateNo = {
    linkWalkStepToJLinkWalkStep(LinkWalkStep(lwsBucket, lwsTag, false)).getKeep must_== Accumulate.NO
  }

  def javaImplicitAccumulateDefault = {
    linkWalkStepToJLinkWalkStep(LinkWalkStep(lwsBucket, lwsTag)).getKeep must_== Accumulate.DEFAULT
  }

  def tuple3implicit = {
    (tuple3ToLinkWalkStep(lwsBucket, lwsTag, true)) === lws
  }

  def tuple3explicit = {
    (lwsBucket, lwsTag, true).toLinkWalkStep === lws
  }

  def tuple2implicit = {
    (tuple2ToLinkWalkStep(lwsBucket, lwsTag)) === LinkWalkStep(lwsBucket, lwsTag)
  }

  def tuple2explicit = {
    (lwsBucket, lwsTag).toLinkWalkStep === LinkWalkStep(lwsBucket, lwsTag)
  }

  def testMultiply = {
    (lws * 2).list must haveTheSameElementsAs(lws :: lws :: Nil)
  }
  
  def testMultiplyList = {
    (lws * 2 * 2).list must_== (lws * 4).list
  }
  
  def testMultiplyLessThan1 = {
    (lws * 0).list must haveTheSameElementsAs(lws :: Nil)
  }

  def testMultiplyImplicit = {
    ((lwsBucket, lwsTag, true) * 2).list must haveSize(2)
  }

  def testAdd2 = {
    val lws2 = LinkWalkStep("bucket", "tag", true)
    (lws step lws2).list must contain(lws, lws2).only.inOrder
  }
  
  def testAdd4 = {
    val bucket2 = "bucket2"; val lws2 = (bucket2, "tag")
    val bucket3 = "bucket3"; val lws3 = LinkWalkStep(bucket3, "tag")
    val bucket4 = "bucket4"; val lws4 = (bucket4, "tag")
    ((lws step lws2 step lws3 step lws4) map { _.bucket }).list must contain(lwsBucket, bucket2, bucket3, bucket4).only.inOrder
  }

  def testAddMultMix = {
    val bucket2 = "bucket2"; val lws2 = (bucket2, "tag")
    val bucket3 = "bucket3"; val lws3 = LinkWalkStep(bucket3, "tag")
    ((lws step lws2 * 2 step lws3 * 3) map { _.bucket }).list must contain(lwsBucket, bucket2, bucket2, bucket3, bucket3, bucket3).only.inOrder
  }

  def testAddMultOperatorMix = {
    val bucket2 = "bucket2"; val lws2 = (bucket2, "tag")
    val bucket3 = "bucket3"; val lws3 = LinkWalkStep(bucket3, "tag")
    ((lws --> lws2 * 3 --> lws3 * 2) map { _.bucket }).list must contain(lwsBucket, bucket2, bucket2, bucket2, bucket3, bucket3).only.inOrder
  }

  object walkingSimpleObject {
    class WalkSpecExtractor extends util.MockitoArgumentExtractor[LinkWalkSpec]

    val testKey = "test-key"
    val testContentType = "text/plain"
    val mockVClock = mock[VClock]
    val obj = new ReadObject(
      testKey,
      lwsBucket,
      testContentType,
      mockVClock,
      "".getBytes
    )

    def testStartBucket = {
      val (rawClient, bucket) = createClientAndBucket
      val lws2 = LinkWalkStep("bucket", "tag")
      val walkAction = (bucket, obj) linkWalk lws --> lws2
      val extractor = new WalkSpecExtractor
      rawClient.linkWalk(MM.argThat(extractor)) returns createWalkResult()
      walkAction.unsafePerformIO()

      extractor.argument must beSome.like {
        case spec => spec.getStartBucket must beEqualTo(lwsBucket)
      }
    }

    def testStartKey = {
      val (rawClient, bucket) = createClientAndBucket
      val lws2 = LinkWalkStep("bucket", "tag")
      val walkAction = (bucket, obj) linkWalk lws --> lws2
      val extractor = new WalkSpecExtractor
      rawClient.linkWalk(MM.argThat(extractor)) returns createWalkResult()
      walkAction.unsafePerformIO()

      extractor.argument must beSome.like {
        case spec => spec.getStartKey must beEqualTo(testKey)
      }
    }

    def testStartSteps = {
      val (rawClient, bucket) = createClientAndBucket
      val lws2 = LinkWalkStep("bucket", "tag")
      val walkAction = (bucket, obj) linkWalk lws --> lws2
      val extractor = new WalkSpecExtractor
      rawClient.linkWalk(MM.argThat(extractor)) returns createWalkResult()
      walkAction.unsafePerformIO()

      import scala.collection.JavaConverters._
      extractor.argument must beSome.like {
        case spec => {
          val stepsIterator = spec.asScala
          val stepsConverted = stepsIterator.toList map { step => LinkWalkStep(step.getBucket, step.getTag, step.getKeep) }
          val actualAndExpected: List[(LinkWalkStep, LinkWalkStep)] = stepsConverted.zip(List(lws, lws2))
          ((ae: (LinkWalkStep, LinkWalkStep)) => (ae._1 === ae._2) must beTrue).forall(actualAndExpected)
        }
      }
    }

    def testSimpleResults = {
      val expectedValues = List(List("value1", "value2"), List("value3"), List("value4"))
      val mockedValues = expectedValues.map { _.zipWithIndex map { vi => mockRiakObj(lwsBucket, vi._2.toString, vi._1.getBytes, "text/plain", "vclock") } }
      val (rawClient, bucket) = createClientAndBucket
      val walkAction = (bucket, obj) linkWalk lws
      rawClient.linkWalk(MM.isA(classOf[LinkWalkSpec])) returns createWalkResult(mockedValues)

      walkAction.unsafePerformIO().toList map { _.toList map { _.stringValue } } must haveTheSameElementsAs(expectedValues)
    }
    

    case class TestDomainObject(key: String, value: String)
    def testDomainResults = {
      val failKey = "key4"
      implicit val converter = ScaliakConverter.newConverter[TestDomainObject](
        scObj => if (scObj.key === failKey) (new  Exception("conversion fail")).failNel else TestDomainObject(scObj.key, scObj.stringValue).successNel,
        dObj => WriteObject(dObj.key, dObj.value.getBytes)
      )
      val expectedValues = List(List(TestDomainObject("key1", "value1"), TestDomainObject("key2", "value2")), List(TestDomainObject("key3", "value3")))
      val mockedValues = expectedValues.map { _ map { dObj => mockRiakObj(lwsBucket, dObj.key, dObj.value.getBytes, "text/plain", "vclock") } }
      val finalMockedValues = List(mockRiakObj(lwsBucket, failKey, "".getBytes, "text/plain", "vclock")) :: mockedValues
      val (rawClient, bucket) = createClientAndBucket
      val walkAction = (bucket, obj) linkWalk lws
      rawClient.linkWalk(MM.isA(classOf[LinkWalkSpec])) returns createWalkResult(finalMockedValues)

      walkAction.unsafePerformIO().toList map { _.toList } must haveTheSameElementsAs(expectedValues)
    }

    def createWalkResult(ls: List[List[IRiakObject]] = Nil) = {
      import scala.collection.JavaConverters._
      new WalkResult() {
        def iterator() = (ls map { _.asJavaCollection }).asJava.iterator()
      }
    }

    def createClientAndBucket: (RawClient, ScaliakBucket) = {
      val rawClient = mock[RawClientWithStreaming]
      val bucket = new ScaliakBucket(
        rawClientOrClientPool = Left(rawClient),
        name = lwsBucket,
        allowSiblings = false,
        lastWriteWins = false,
        nVal = 3,
        backend = None,
        smallVClock = 1,
        bigVClock = 2,
        youngVClock = 3,
        oldVClock = 4,
        precommitHooks = Nil,
        postcommitHooks = Nil,
        rVal = new Quorum(2),
        wVal = new Quorum(2),
        rwVal = new Quorum(0),
        dwVal = new Quorum(0),
        prVal = new Quorum(0),
        pwVal = new Quorum(0),
        basicQuorum = false,
        notFoundOk = false,
        chashKeyFunction = mock[NamedErlangFunction],
        linkWalkFunction = mock[NamedErlangFunction],
        isSearchable = false
      )

      (rawClient, bucket)
    }

  }
  // SPECS2 HACK TO ALLOW CALLING OF SCALAZ === WITHOUT AMBIGIOUS IMPLICITS
  override def canBeEqual[T](t: => T) = super.canBeEqual(t)

}
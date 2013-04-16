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
import scalaz.effect.IO
import com.basho.riak.client.query.functions.NamedErlangFunction
import com.basho.riak.client.cap.{UnresolvedConflictException, VClock, Quorum}
import org.mockito.{Matchers => MM}
import com.stackmob.scaliak._
import com.basho.riak.client.raw._
import com.basho.riak.client.{RiakLink, IRiakObject}
import java.util.Date
import com.basho.riak.client.query.indexes.{IntIndex, BinIndex}
import query.indexes.{IntValueQuery, BinValueQuery, IndexQuery}

// TODO: these specs really cover both ReadObject and ScaliakBucket, they should be split up
class ScaliakBucketSpecs extends Specification with Mockito with util.MockRiakUtils { def is =
  "Scaliak Bucket".title                                                            ^
  """
  This class provides the primary functionality for fetching data
  from and storing data in Riak.
  """                                                                               ^
                                                                                    p^
  "Fetching Data"                                                                   ^
    "Fetching with No Conversion"                                                   ^
      "When the key being fetched is missing returns None"                          ! skipped ^
      "When the key being fetched exists"                                           ^
        "When there are no conflicts"                                               ^
          "returns a ReadObject whose key is the same as the one fetched"        ! simpleFetch.someWKey ^
          "can get the stored bytes by calling getBytes on the returned object"     ! simpleFetch.testGetBytes ^
          "calling stringValue on the returned object returns the string value"     ! simpleFetch.testStringValue ^
          "the returned object has the same bucket name as the one used to fetch it"! simpleFetch.testBucketName ^
          "the returned object has a vclock"                                        ! simpleFetch.testVClock ^
          "calling vclockString returns the vclock as a string"                     ! simpleFetch.testVClockStr ^
          "the returned object has a vTag"                                          ! simpleFetch.testVTag ^
          "the returned object has a lastModified timestamp"                        ! simpleFetch.testLastModified ^
          "the returned object has a content type"                                  ! simpleFetch.tContentType ^
          "if the fetched object has an empty list of links"                        ^
            "links returns None"                                                    ! simpleFetch.testEmptyLinksIsNone ^
            "hasLinks returns false"                                                ! simpleFetch.testEmptyLinkHasLinksIsFalse ^
            "numLinks returns 0"                                                    ! simpleFetch.testEmptyLinksReturnsZeroNumLinks ^
            "containsLink returns false for any link"                               ! simpleFetch.testEmptyLinksContainsLinkReturnsFalse ^p^
          "if the fetched object has a non-empty list of links"                     ^
            "links returns Some -- a non-empty list of links converted to scaliak"  ! nonEmptyLinkFetch.testConvertedLinks ^
            "hasLinks returns some"                                                 ! nonEmptyLinkFetch.testHasLinks ^
            "numLinks returns the number of links in the fetched object"            ! nonEmptyLinkFetch.testNumLinks ^
            "containsLink returns true for a link that exists"                      ! nonEmptyLinkFetch.testContainsLinkTrueIfContained ^
            "containsLink returns false for a link that does not exist"             ! nonEmptyLinkFetch.testContainsLinkFalseIfNotContained ^p^
          "if the fetched object does not have metadata"                            ^
            "metadata returns an empty Map[String, String]"                         ! simpleFetch.testEmptyMetadataMap ^
            "hasMetadata returns false"                                             ! simpleFetch.testEmptyMetadataHasMetadataReturnsFalse ^
            "containsMetadata returns false for any key"                            ! simpleFetch.testEmptyMetadataContainsMetadataReturnsFalse ^
            "getMetadata returns None for any key"                                  ! simpleFetch.testEmptyMetadataGetReturnsNone ^
          "if the fetched object has metadata"                                      ^
            "metadata returns a Map[String, String] w/ data from fetched obj"       ! nonEmptyMetadataFetch.testHasCorrectMetadata ^
            "hasMetadata returns true"                                              ! nonEmptyMetadataFetch.testHasMetadataIsTrue ^
            "containsMetadata returns true for a key in the metadata map"           ! nonEmptyMetadataFetch.testContainsMetadataForExistingKey ^
            "containsMetadata returns false for a key in the metadata map"          ! nonEmptyMetadataFetch.testContainsMetadataForMissingKey ^
            "getMetadata returns Some containing the string if key exists"          ! nonEmptyMetadataFetch.testGetMetadataForExistingKey ^
            "getMetadata returns None if key does not exist"                        ! nonEmptyMetadataFetch.testGetMetadataForMissingKey ^p^
          "if the fetched object does not have bin indexes"                         ^
            "binIndexes is empty"                                                   ! simpleFetch.testEmptyBinIndexes ^
            "binIndex returns None for any index name"                              ! simpleFetch.testEmptyBinIndexesGetIndexReturnsNone ^p^
          "if the fetched object has bin indexes"                                   ^
            "binIndexes returns a Map[BinIndex,Set[String]] containing all bin idxs"! nonEmptyBinIndexesFetch.testHasAll ^
            "binIndex returns Some(Set[String]) for an index string that exists"    ! nonEmptyBinIndexesFetch.testGetBinIndexExists ^
            "binIndex returns None for an index string that d.n.e"                  ! nonEmptyBinIndexesFetch.testGetBinIndexMissing ^p^
          "if the fetched object does not have int indexes"                         ^
            "intIndexes is empty"                                                   ! simpleFetch.testEmptyIntIndexes ^
            "intIndex returns None for any index name"                              ! simpleFetch.testEmptyIntIndexesGetReturnsNone ^p^
          "if the fetched object has int indexes"                                   ^
            "intIndexes returns a Map[IntIndex,Set[Int]] containing all int idxs"   ! nonEmptyIntIndexesFetch.testHasAll ^
            "intIndex returns Some(Set[Int]) for an index string that exists"       ! nonEmptyIntIndexesFetch.testGetIntIndexExists ^
            "intIndex returns None for an index that d.n.e"                         ! nonEmptyIntIndexesFetch.testGetIntIndexMissing ^
                                                                                    p^
        "When there are conflicts"                                                  ^
          "the default conflict resolver throws an UnresolvedConflictException"     ! conflictedFetch.testDefaultConflictRes ^
                                                                                    p^p^p^
    "Fetching with Conversion"                                                      ^
      "When the key being fetched is missing returns None"                          ! skipped ^
      "when the key being fetched exists"                                           ^
        "when there are no conflicts"                                               ^
          "when the conversion succeeds"                                            ^
            "returns the object of type T when converter is supplied explicitly"    ! simpleFetch.testConversionExplicit ^
            "returns the object of type T when converter is supplied implicitly"    ! simpleFetch.testConversionImplicit ^
                                                                                    p^p^p^p^
    "Setting the r value for the request"                                           ^
      "if not set the generated meta has a null r value"                            ! riakArguments.testDefaultR ^
      "if set the generated meta has the given r value"                             ! riakArguments.testPassedR  ^p^
    "Setting the pr value for the request"                                          ^
      "if not set the generated meta has a null pr value"                           ! riakArguments.testDefaultPR ^
      "if set the generated meta has the given pr value"                            ! riakArguments.testPassedPR  ^p^
    "Setting the notFoundOk value for the request"                                  ^
      "if not set the generated meta has a null notFoundOk value"                   ! riakArguments.testDefaultNotFoundOk ^
      "if set the generated meta has the given notFoundOk value"                    ! riakArguments.testPassedNotFoundOk  ^p^
    "Setting the basicQuorum value for the request"                                 ^
      "if not set the generated meta has a null basicQuorum value"                  ! riakArguments.testDefaultBasicQuorum ^
      "if set the generated meta has the given basicQuorum value"                   ! riakArguments.testPassedBasicQuorum  ^p^
    "Setting the returnDeletedVClock value for the request"                         ^
      "if not set the generated meta has a null returnDeletedVClock value"          ! riakArguments.testDefaultReturnedVClock ^
      "if set the generated meta has the given returnDeletedVClock value"           ! riakArguments.testPassedReturnedVClock  ^p^
    "Setting the modifiedSince value for the request"                               ^
      "if not set the generated meta has a null modifiedSince value"                ! riakArguments.testDefaultModifiedSince ^
      "if set the generated meta has the given modifiedSince value"                 ! riakArguments.testPassedModifiedSince  ^p^
    "Setting the ifModified value for the request"                                  ^
      "if not set the generated meta has a null ifModified value"                   ! riakArguments.testDefaultIfModified ^
      "if set the generated meta has the given ifModified value"                    ! riakArguments.testPassedIfModified  ^
    "fetchDangerous is fetch but without default exception handling"                ! simpleFetch.testDangerous ^
    "fetchUnsafe calls unsafePerformIO immediately"                                 ! simpleFetch.testUnsafe ^
                                                                                    endp^
  "Writing Data"                                                                    ^
    "With No Conversion"                                                            ^
      "When the Key Being Fetched Does Not Exist"                                   ^
        """Given the default "Clobber Mutation" """                                 ^
          "Writes the ReadObject as passed in (converted to an IRiakObject)"     ! writeMissing.performsWrite ^
          "returns Success(None) when return body is false (default)"               ! writeMissing.noReturnBody ^
          "returns successfully with the stored object as a ReadObject instance" ! writeMissingReturnBody.noConversion ^
                                                                                    p^
        "Given a mutator other than the default"                                    ^
          "Writes the ReadObject as returned from the mutator"                   ! writeMissing.customMutator ^
                                                                                    p^p^
      "When the Key Being Fetched Exists"                                           ^
        """Given the default "Clobber Mutator" """                                  ^
          "Writes the ReadObject as passed in (converted to an IRiakObject)"     ! writeExisting.performsWrite ^
          "returns Success(None) when return body is false (default)"               ! writeExisting.noReturnBody ^
          "returns successfully with the stored object as a ReadObject instance" ! writeExistingReturnBody.noConversion ^
                                                                                    p^
        "Given a mutator other than the default"                                    ^
          "Writes the ReadObject as returned from the mutator"                   ! writeExisting.customMutator ^
                                                                                    p^p^
      "Can update the links on an object"                                           ! writeExisting.testUpdateLinks ^
      "Can update the metadata on an object"                                        ! writeExisting.testUpdateMetadata ^
      "Can update the binary indexes on an object"                                  ! writeExisting.testUpdateBinIndexes ^
      "Can update the integer indexes on an object"                                 ! writeExisting.testUpdateIntIndexes ^
                                                                                    p^
    "With Conversion"                                                               ^
      "When the Key Being Fetched Does Not Exist"                                   ^
        """Given the default "Clobber Mutation" """                                 ^
          "Writes object converted to a WriteObject then a ReadObject"  ! writeMissing.domainObject ^p^
        "Given a mutator other than the default"                                    ^
          "Writes the object as returned from the mutator, converting it afterwards"! writeMissing.domainObjectCustomMutator ^p^p^
      "When the Key Being Fetched Exists"                                           ^
        """Given the default "Clobber Mutation" """                                  ^
          "Writes object converted to a WriteObject then a ReadObject"  ! writeExisting.domainObject ^p^
        "Given a mutator other than the default"                                    ^
          "Writes the object as returned from the mutator, converting it afterwards"! writeExisting.domainObjectCustomMutator ^
                                                                                    p^p^p^
    "Without Reading First"                                                         ^
      "Does not call read before writing the data"                                  ! writeOnly.writesButNoRead ^
      "Preserves the VClock from the generated WriteObject if there is one"! writeOnly.usesPartialScaliakObjectVClock ^
      "Setting the w value for the request"                                         ^
        "if not set the generated meta has a null w value"                          ! riakArguments.testDefaultWPut ^
        "if set the generated meta has the given w value"                           ! riakArguments.testPassedWPut ^p^
      "Setting the pw value for the request"                                        ^
        "if not set the generated meta has a null pw value"                         ! riakArguments.testDefaultPWPut ^
        "if set the generated meta has the given pw value"                          ! riakArguments.testPassedPWPut ^p^
      "Setting the dw value for the request"                                        ^
        "if not set the generated meta has a null dw value"                         ! riakArguments.testDefaultDWPut ^
        "if set the generated meta has the given dw value"                          ! riakArguments.testPassedDWPut ^p^
      "Setting the return body value"                                               ^
        "if not set the generated meta has a false return body"                     ! riakArguments.testDefaultReturnBodyPut ^
        "if set the generated meta has the given return body"                       ! riakArguments.testPassedReturnBodyPut ^p^
                                                                                    p^
    "Read Properties"                                                               ^
      "Setting the r value for the request"                                         ^
        "if not set the generated meta has a null r value"                          ! riakArguments.testWriteDefaultR ^
        "if set the generated meta has the given r value"                           ! riakArguments.testPassedRWrite ^p^
      "Setting the pr value for the request"                                        ^
        "if not set the generated meta has a null pr value"                         ! riakArguments.testWriteDefaultPR ^
        "if set the generated meta has the given pr value"                          ! riakArguments.testPassedPRWrite ^p^
      "Setting the notFoundOk value for the request"                                ^
        "if not set the generated meta has a null notFoundOk value"                 ! riakArguments.testWriteDefaultNotFoundOk ^
        "if set the generated meta has the given notFoundOk value"                  ! riakArguments.testPassedNotFoundOkWrite ^p^
      "Setting the basicQuorum value for the request"                               ^
        "if not set the generated meta has a null basicQuorum value"                ! riakArguments.testWriteDefaultBasicQuorum ^
        "if set the generated meta has the given basicQuorum value"                 ! riakArguments.testPassedBasicQuorumWrite ^p^
      "Setting the returnDeletedVClock value for the request"                       ^
        "if not set the generated meta has a null returnDeletedVClock value"        ! riakArguments.testWriteDefaultReturnedVClock ^
        "if set the generated meta has the given returnDeletedVClock value"         ! riakArguments.testPassedReturnVClockWrite ^p^p^
    "Write Properties"                                                              ^
      "Setting the w value for the request"                                         ^
        "if not set the generated meta has a null w value"                          ! riakArguments.testDefaultW ^
        "if set the generated meta has the given w value"                           ! riakArguments.testPassedW ^p^
      "Setting the pw value for the request"                                        ^
        "if not set the generated meta has a null pw value"                         ! riakArguments.testDefaultPW ^
        "if set the generated meta has the given pw value"                          ! riakArguments.testPassedPW ^p^
      "Setting the dw value for the request"                                        ^
        "if not set the generated meta has a null dw value"                         ! riakArguments.testDefaultDW ^
        "if set the generated meta has the given dw value"                          ! riakArguments.testPassedDW ^p^
      "Setting the return body value"                                               ^
        "if not set the generated meta has a false return body"                     ! riakArguments.testDefaultReturnBody ^
        "if set the generated meta has the given return body"                       ! riakArguments.testPassedReturnBody ^p^
      "Setting if none match"                                                       ^
        "if not set the generated meta has a null array of etags"                   ! riakArguments.testDefaultIfNoneMatch ^
        "if set the generated meta has a array of etags containing only the vtag"   ! riakArguments.testPassedIfNoneMatch ^p^
      "Setting if not modified"                                                     ^
        "if not set the generated meta has a null last modified timestamp"          ! riakArguments.testDefaultIfNotModified ^
        "if set the genereated meta has the last modified timestamp set"            ! riakArguments.testPassedIfNotModified ^
                                                                                    endp^
  "Deleting Data"                                                                   ^
    "By Key"                                                                        ^
      "Uses the raw client passing in the bucket name, key and delete meta"         ! deleteByKey.test ^p^
    "By ReadObject"                                                              ^
      "Deletes the object by its key"                                               ! deleteScaliakObject.test ^p^
    "By Domain Object"                                                              ^
      "Deletes the object by its key"                                               ! deleteDomainObject.test ^p^
    "If fetch before delete is true (defaults to false)"                            ^
      "Adds the returned vClock to the delete meta"                                 ! deleteWithFetchBefore.testAddsVclock ^
                                                                                    endp^
  "Fetching by Index"                                                               ^
    "by value"                                                                      ^
      "if the value is a string a BinValueQuery is performed"                       ! fetchIndexByValue.testBinValueGeneratesBinQuery ^
      "if the value is an integer a IntValueQuery is performed"                     ! fetchIndexByValue.testIntValueGeneratesIntQuery ^
      "returns the results of the query, a List[String] as returned by the client"  ! fetchIndexByValue.testReturnsKeys ^
                                                                                    end


  // TODO: updating metadata

  class DummyDomainObject(val someField: String)
  val dummyWriteVal = "dummy"
  val dummyDomainConverter = ScaliakConverter.newConverter[DummyDomainObject](
    o => (new DummyDomainObject(o.key)).successNel[Throwable],
    o => WriteObject(o.someField, dummyWriteVal.getBytes)
  )
  val mutationValueAddition = "abc"
  val dummyDomainMutation = ScaliakMutation.newMutation[DummyDomainObject] {
    (mbOld, newObj) => {
      new DummyDomainObject(newObj.someField + mutationValueAddition)
    }
  }

  object fetchIndexByValue extends context {
    val rawClient = mock[RawClientWithStreaming] // not used in these tests
    val bucket = createBucket // not used in these tests

    class IndexQueryExtractor extends util.MockitoArgumentExtractor[IndexQuery]

    val testIdx = "idx1"

    def testBinValueGeneratesBinQuery = {
      val client = mock[RawClientWithStreaming]
      val b = createBucketWithClient(client)
      val testBinVal = "someval"

      val extractor = new IndexQueryExtractor
      val testResults: java.util.List[String] = new java.util.LinkedList[String]()
      testResults.add("key1")
      testResults.add("key12")
      client.fetchIndex(MM.argThat(extractor)) returns testResults

      b.fetchIndexByValue(index = testIdx, value = testBinVal).unsafePerformIO().toOption // execute
      extractor.argument must beSome.like {
        case obj: BinValueQuery => (obj.getBucket must beEqualTo(bucket.name)) and (obj.getIndex must beEqualTo(testIdx + "_bin")) and (obj.getValue must beEqualTo(testBinVal))
      }
    }

    def testIntValueGeneratesIntQuery = {
      val client = mock[RawClientWithStreaming]
      val b = createBucketWithClient(client)
      val testIntVal = 1

      val extractor = new IndexQueryExtractor
      val testResults: java.util.List[String] = new java.util.LinkedList[String]()
      client.fetchIndex(MM.argThat(extractor)) returns testResults

      b.fetchIndexByValue(index = testIdx, value = testIntVal).unsafePerformIO().toOption // execute
      extractor.argument must beSome.like {
        case obj: IntValueQuery => (obj.getBucket must beEqualTo(bucket.name)) and (obj.getIndex must beEqualTo(testIdx + "_int")) and (obj.getValue must beEqualTo(testIntVal))
      }
    }

    def testReturnsKeys = {
      import scala.collection.JavaConverters._
      val client = mock[RawClientWithStreaming]
      val b = createBucketWithClient(client)
      val indexVal = "string"
      val testResults: java.util.List[String] = new java.util.LinkedList[String]()
      testResults.add("1")
      testResults.add("2")
      client.fetchIndex(any) returns testResults

      b.fetchIndexByValue(index = testIdx, value = indexVal).map(_.toOption).unsafePerformIO() must beSome.like {
        case res => res must beEqualTo(testResults.asScala.toList)
      }
    }

  }

  object deleteWithFetchBefore extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    class DeleteMetaArgExtractor extends util.MockitoArgumentExtractor[DeleteMeta]
    
    lazy val result = bucket.deleteByKey(testKey, fetchBefore = true).unsafePerformIO()

    val extractor = new DeleteMetaArgExtractor
    (rawClient.delete(MM.eq(testBucket), MM.eq(testKey), MM.argThat(extractor))
      throws new NullPointerException("can't stub void methods, tests don't depend on the result anyway"))

    val mockHeadVClock = mock[VClock]
    mockHeadVClock.asString returns "fetched"
    val mockHeadResponse = mockRiakResponse(Array())    
    mockHeadResponse.getVclock returns mockHeadVClock
    
    rawClient.head(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockHeadResponse
    
    def testAddsVclock = {
      result
      
      extractor.argument must beSome.like {
        case meta => meta.getVclock.asString must beEqualTo("fetched")
      }
    }
  }

  object deleteDomainObject extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val obj = new DummyDomainObject(testKey)
    implicit val converter = dummyDomainConverter
    lazy val result = bucket.delete(obj).unsafePerformIO()

    def test = {
      result
      there was one(rawClient).delete(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[DeleteMeta]))
    }
  }

  object deleteScaliakObject extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val obj = ReadObject(testKey, testBucket, testContentType, mock[VClock], "".getBytes)
    lazy val result = bucket.delete(obj).unsafePerformIO()

    def test = {
      result
      there was one(rawClient).delete(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[DeleteMeta]))
    }
  }

  object deleteByKey extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    lazy val result = bucket.deleteByKey(testKey).unsafePerformIO()

    def test = {
      result
      there was one(rawClient).delete(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[DeleteMeta]))
    }
  }

  object writeExistingReturnBody extends writeBase {
    val mock2VClockStr = "vclock2"
    val mockStoreObj = mockRiakObj(testBucket, testKey, testStoreObject.getBytes, testContentType, mock2VClockStr)
    val mockStoreResponse = mockRiakResponse(Array(mockStoreObj))

    override lazy val result = bucket.store(testStoreObject, returnBody = true).unsafePerformIO()

    val mock1VClockStr = "vclock1"
    val mockFetchObj = mockRiakObj(testBucket, testKey, "abc".getBytes, testContentType, mock1VClockStr)
    val mockFetchVClockStr = mock1VClockStr
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockRiakResponse(Array(mockFetchObj))

    val extractor = new IRiakObjExtractor
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

    def noConversion = {
      val r = result.toOption | None
      r aka "the object returned from store or None" must beSome[ReadObject].which { obj =>
        obj.stringValue == testStoreObject.stringValue && obj.vClockString == mock2VClockStr
      }
    }
  }

  object writeMissingReturnBody extends writeBase {
    val mock2VClockStr = "vclock2"
    val mockStoreObj = mockRiakObj(testBucket, testKey, testStoreObject.getBytes, testContentType, mock2VClockStr)
    val mockStoreResponse = mockRiakResponse(Array(mockStoreObj))
    val mockFetchVClockStr = ""
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockRiakResponse(Array())

    override lazy val result = bucket.store(testStoreObject, returnBody = true).unsafePerformIO()

    val extractor = new IRiakObjExtractor
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

    def noConversion = {
      val r = result.toOption | None
      r aka "the object returned from store or None" must beSome[ReadObject].which { obj =>
        obj.stringValue == testStoreObject.stringValue && obj.vClockString == mock2VClockStr
      }
    }
  }

  object writeExisting extends writeBase {
    val mock1Bytes = Array[Byte](1, 2)
    val mock1VClockStr = "a vclock"
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, mock1Bytes, testContentType, mock1VClockStr)

    val mockResponse = mockRiakResponse(Array(mockRiakObj1))
    mockResponse.getVclock returns mockRiakObj1.getVClock
    val mockFetchVClockStr = mockRiakObj1.getVClock.asString

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

    val extractor = new IRiakObjExtractor
    val mockStoreResponse = mockRiakResponse(Array())
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse // TODO: these should not return null
    
    def testUpdateLinks = {
      result // execute call
      
      extractor.argument must beSome.like {
        case obj => obj.getLinks.toArray must haveSize(testStoreObject.numLinks)
      }
    }
    
    def testUpdateMetadata = {
      import scala.collection.JavaConverters._
      result // execute call
      
      extractor.argument must beSome.like {
        case obj => obj.getMeta.asScala.toMap must beEqualTo(testStoreObject.metadata)
      }
    }

    def testUpdateBinIndexes = {
      import scala.collection.JavaConverters._
      result // execute call

      extractor.argument must beSome.like {
        case obj => obj.allBinIndexes.asScala.mapValues(_.asScala.toSet).toMap must beEqualTo(testStoreObject.binIndexes)
      }
    }

    def testUpdateIntIndexes = {
      import scala.collection.JavaConverters._
      result // execute call

      extractor.argument must beSome.like {
        case obj => obj.allIntIndexesV2.asScala.mapValues(_.asScala.toSet).toMap must beEqualTo(testStoreObject.intIndexes)
      }
    }

    def customMutator = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse
      val newExtractor = new IRiakObjExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      var fakeValue: String = "fail"
      implicit val mutator = ScaliakMutation.newMutation[ReadObject] {
        (o: Option[ReadObject], n: ReadObject) => {
          fakeValue = "custom"
          n.copy(bytes = fakeValue.getBytes)
        }
      }
      newBucket.store(testStoreObject).unsafePerformIO()

      newExtractor.argument must beSome.like {
        case obj => obj.getValueAsString must_== fakeValue
      }
    }

    def domainObject = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new IRiakObjExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO()
      
      newExtractor.argument must beSome.like {
        case o => o.getValueAsString must beEqualTo(dummyWriteVal)
      }
    }

    def domainObjectCustomMutator = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new IRiakObjExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      implicit val mutation = dummyDomainMutation
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO()

      newExtractor.argument must beSome.like {
        case o => o.getKey must beEqualTo(testKey + mutationValueAddition)
      }
    }
  }

  object writeMissing extends writeBase {
    val mockResponse = mockRiakResponse(Array())
    mockResponse.getVclock returns null
    val mockFetchVClockStr = ""
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

    val extractor = new IRiakObjExtractor
    val mockStoreResponse = mockRiakResponse(Array())
    rawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse
    
    
    def customMutator = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse
      val newExtractor = new IRiakObjExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      var fakeValue: String = "fail"
      implicit val mutator = ScaliakMutation.newMutation[ReadObject] {
        (o: Option[ReadObject], n: ReadObject) => {
          fakeValue = "custom"
          n.copy(bytes = fakeValue.getBytes)
        }
      }
      newBucket.store(testStoreObject).unsafePerformIO()
            
      newExtractor.argument must beSome.like {
        case obj => obj.getValueAsString must_== fakeValue
      }
    }

    def domainObject = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new IRiakObjExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO()

      newExtractor.argument must beSome.like {
        case o => o.getValueAsString must beEqualTo(dummyWriteVal)
      }
    }

    def domainObjectCustomMutator = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResponse

      val newExtractor = new IRiakObjExtractor
      newRawClient.store(MM.argThat(newExtractor), MM.isA(classOf[StoreMeta])) returns mockStoreResponse

      implicit val converter = dummyDomainConverter
      implicit val mutation = dummyDomainMutation
      newBucket.store(new DummyDomainObject(testKey)).unsafePerformIO()

      newExtractor.argument must beSome.like {
        case o => o.getKey must beEqualTo(testKey + mutationValueAddition)
      }
    }
  }

  object writeOnly extends writeBase {
    val mockFetchVClockStr = "test"
    val extractor = new IRiakObjExtractor

    def writesButNoRead = {
      bucket.put(testStoreObject).unsafePerformIO() // execute write only operation

      there was no(rawClient).fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) `then`
        one(rawClient).store(MM.isA(classOf[IRiakObject]), MM.isA(classOf[StoreMeta]))
    }

    def usesPartialScaliakObjectVClock = {
      val newRawClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newRawClient)
      newRawClient.store(MM.argThat(extractor), MM.isA(classOf[StoreMeta])) returns null

      val mockVClock = mock[VClock]
      mockVClock.asString returns "test"
      implicit val testConverter = ScaliakConverter.newConverter[ReadObject](
        scObj => (new Exception("who cares")).failNel,
        obj => WriteObject(obj.key, obj.bytes, vClock = mockVClock.some)
      )

      newBucket.put(testStoreObject).unsafePerformIO()

      extractor.argument must beSome.like {
        case o => o.getVClock.asString must_== "test"
      }
    }
  }

  trait writeBase extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket
    def mockFetchVClockStr: String
    def extractor: IRiakObjExtractor

    class CustomMutation extends ScaliakMutation[ReadObject] {
      val fakeValue = "custom"
      def apply(old: Option[ReadObject], newObj: ReadObject) = {
        newObj.copy(bytes = fakeValue.getBytes)
      }
    }
    

    lazy val result = bucket.store(testStoreObject).unsafePerformIO()

    val mockVClock = mock[VClock]
    mockVClock.getBytes returns Array[Byte]()
    mockVClock.asString returns ""
    val testStoreObject = new ReadObject(
      testKey,
      testBucket,
      testContentType,
      mockVClock,
      "".getBytes,
      links = nel(ScaliakLink("test", "test", "test")).some,
      metadata = Map("m1" -> "v1", "m2" -> "v2"),
      binIndexes = Map(BinIndex.named("idx1") -> Set("a", "b"), BinIndex.named("idx2") -> Set("d", "e")),
      intIndexes = Map(IntIndex.named("idx1") -> Set(1), IntIndex.named("idx2") -> Set(3,4))
    )

    class IRiakObjExtractor extends util.MockitoArgumentExtractor[IRiakObject]
    
    def performsWrite = {
      result

      extractor.argument must beSome.which { obj =>
        (obj.getKey == testKey && obj.getContentType == testContentType &&
          obj.getBucket == testBucket && obj.getValueAsString == testStoreObject.stringValue &&
          ~(Option(obj.getVClockAsString)) == mockFetchVClockStr) // in this last step we are checking if the vclock is null by using string Zero value when it is
      }
    }

    def noReturnBody = {
      result.toOption must beSome.like {
        case o => o must beNone
      }
    }

  }

  object riakArguments extends context {
    // these aren't really used
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket


    val testVTag = "test"
    val lastModified = new java.util.Date(System.currentTimeMillis)
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, "".getBytes("utf-8"), "text/plain", "vclock", vTag = testVTag, lastModified = lastModified)

    val testStoreObject = new ReadObject(
      testKey,
      testBucket,
      testContentType,
      null,
      "".getBytes,
      links = nel(ScaliakLink("test", "test", "test")).some,
      metadata = Map("m1" -> "v1", "m2" -> "v2"),
      vTag = testVTag,
      lastModified = lastModified
    )


    class FetchMetaExtractor extends util.MockitoArgumentExtractor[FetchMeta]
    class StoreMetaExtractor extends util.MockitoArgumentExtractor[StoreMeta]
    
    def initExtractor(rawClient: RawClient) = {
      val extractor = new FetchMetaExtractor
      rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.argThat(extractor)) returns mockRiakResponse(Array())
      extractor
    } 
    
    def initWriteExtractor(rawClient: RawClient) = {
      initExtractor(rawClient)
      val extractor = new StoreMetaExtractor
      rawClient.store(MM.isA(classOf[IRiakObject]), MM.argThat(extractor)) returns null
      extractor
    }
    
    def testArg(f: ScaliakBucket => IO[ValidationNel[Throwable, Option[ReadObject]]]): FetchMetaExtractor = {
      val rawClient = mock[RawClientWithStreaming]
      val ex = initExtractor(rawClient)
      val bucket = createBucketWithClient(rawClient)
      f(bucket).unsafePerformIO()
      ex
    }

    def testWriteArg(f: ScaliakBucket => IO[ValidationNel[Throwable, Option[ReadObject]]]): StoreMetaExtractor = {
      val rawClient = mock[RawClientWithStreaming]
      val ex = initWriteExtractor(rawClient)
      val bucket = createBucketWithClient(rawClient)
      f(bucket).unsafePerformIO()

      ex
    }

    def testDefaultFetchMetaBase[T](metaProp: FetchMeta => T, f: ScaliakBucket => IO[ValidationNel[Throwable, Option[ReadObject]]]) = {
      testArg(f).argument must beSome.like {
        case meta => metaProp(meta) must beNull
      }
    }
    
    def testDefaultStoreMetaBase[T](metaProp: StoreMeta => T, f: ScaliakBucket => IO[ValidationNel[Throwable, Option[ReadObject]]]) = {
      testWriteArg(f).argument must beSome.like {
        case meta => metaProp(meta) must beNull
      }
    }

    def testDefaultFetchMeta[T](metaProp: FetchMeta => T) = testDefaultFetchMetaBase(metaProp, _.fetch(testKey))
    def testWriteDefaultFetchMeta[T](metaProp: FetchMeta => T) = testDefaultFetchMetaBase(metaProp, _.store(testStoreObject))

        
    def testDefaultStoreMeta[T](metaProp: StoreMeta => T) = testDefaultStoreMetaBase(metaProp, _.store(testStoreObject))
    def testPutDefaultStoreMeta[T](metaProp: StoreMeta => T) = testDefaultStoreMetaBase(metaProp, _.put(testStoreObject))


    def testPassedFetchMeta[T](f: ScaliakBucket => IO[ValidationNel[Throwable, Option[ReadObject]]], metaProp: FetchMeta => T, expected: T) = {
      testArg(f).argument must beSome.like {
        case meta => metaProp(meta) must beEqualTo(expected)
      }
    }

    def testPassedStoreMeta[T](f: ScaliakBucket => IO[ValidationNel[Throwable, Option[ReadObject]]], metaProp: StoreMeta => T, expected: T) = {
      testWriteArg(f).argument must beSome.like {
        case meta => metaProp(meta) must beEqualTo(expected)
      }
    }

    def testDefaultR = testDefaultFetchMeta(_.getR)
    def testDefaultPR = testDefaultFetchMeta(_.getPr)
    def testDefaultNotFoundOk = testDefaultFetchMeta(_.getNotFoundOK)
    def testDefaultBasicQuorum = testDefaultFetchMeta(_.getBasicQuorum)
    def testDefaultReturnedVClock = testDefaultFetchMeta(_.getReturnDeletedVClock)
    def testDefaultModifiedSince = testDefaultFetchMeta(_.getIfModifiedSince)
    def testDefaultIfModified = testDefaultFetchMeta(_.getIfModifiedVClock)

    def testWriteDefaultR = testWriteDefaultFetchMeta(_.getR)
    def testWriteDefaultPR = testWriteDefaultFetchMeta(_.getPr)
    def testWriteDefaultNotFoundOk = testWriteDefaultFetchMeta(_.getNotFoundOK)
    def testWriteDefaultBasicQuorum = testWriteDefaultFetchMeta(_.getBasicQuorum)
    def testWriteDefaultReturnedVClock = testWriteDefaultFetchMeta(_.getReturnDeletedVClock)    

    def testDefaultW = testDefaultStoreMeta(_.getW)
    def testDefaultDW = testDefaultStoreMeta(_.getDw)
    def testDefaultPW = testDefaultStoreMeta(_.getPw)
    def testDefaultIfNoneMatch = testDefaultStoreMeta(_.getEtags)
    def testDefaultIfNotModified = testDefaultStoreMeta(_.getLastModified)
    def testDefaultReturnBody = testWriteArg(_.store(testStoreObject)).argument must beSome.like {
      case meta => meta.getReturnBody must beEqualTo(false)
    }
    
    def testDefaultWPut = testPutDefaultStoreMeta(_.getW)
    def testDefaultDWPut = testPutDefaultStoreMeta(_.getDw)
    def testDefaultPWPut = testPutDefaultStoreMeta(_.getPw)
    def testDefaultReturnBodyPut = testWriteArg(_.put(testStoreObject)).argument must beSome.like {
      case meta => meta.getReturnBody must beEqualTo(false)
    }

    def testPassedR = testPassedFetchMeta(_.fetch(testKey, r = 3), _.getR, new Quorum(3))
    def testPassedPR = testPassedFetchMeta(_.fetch(testKey, pr = 2), _.getPr, new Quorum(2))
    def testPassedNotFoundOk = testPassedFetchMeta(_.fetch(testKey, notFoundOk = true), _.getNotFoundOK, true)
    def testPassedBasicQuorum = testPassedFetchMeta(_.fetch(testKey, basicQuorum = false), _.getBasicQuorum, false)
    def testPassedReturnedVClock = testPassedFetchMeta(_.fetch(testKey, returnDeletedVClock = true), _.getReturnDeletedVClock, true)
    def testPassedModifiedSince = testPassedFetchMeta(_.fetch(testKey, ifModifiedSince = testDate), _.getIfModifiedSince, testDate)
    def testPassedIfModified = testPassedFetchMeta(_.fetch(testKey, ifModified = testVClock), _.getIfModifiedVClock, testVClock)

    def testPassedRWrite = testPassedFetchMeta(_.store(testStoreObject, r = 2), _.getR, new Quorum(2))
    def testPassedPRWrite = testPassedFetchMeta(_.store(testStoreObject, pr = 1), _.getPr, new Quorum(1))
    def testPassedNotFoundOkWrite = testPassedFetchMeta(_.store(testStoreObject, notFoundOk = true), _.getNotFoundOK, true)
    def testPassedBasicQuorumWrite = testPassedFetchMeta(_.store(testStoreObject, basicQuorum = true), _.getBasicQuorum, true)
    def testPassedReturnVClockWrite = testPassedFetchMeta(_.store(testStoreObject, returnDeletedVClock = false), _.getReturnDeletedVClock, false)

    def testPassedW = testPassedStoreMeta(_.store(testStoreObject, w = 2), _.getW, new Quorum(2))
    def testPassedPW = testPassedStoreMeta(_.store(testStoreObject, pw = 3), _.getPw, new Quorum(3))
    def testPassedDW = testPassedStoreMeta(_.store(testStoreObject, dw = 1), _.getDw, new Quorum(1))
    def testPassedReturnBody = testPassedStoreMeta(_.store(testStoreObject, returnBody = true), _.getReturnBody, true)
    def testPassedIfNoneMatch = testPassedStoreMeta(_.store(testStoreObject, ifNoneMatch = true), _.getEtags.headOption, Option(testVTag))
    def testPassedIfNotModified = testPassedStoreMeta(_.store(testStoreObject, ifNotModified = true), _.getLastModified, lastModified)

    def testPassedWPut = testPassedStoreMeta(_.put(testStoreObject, w = 2), _.getW, new Quorum(2))
    def testPassedPWPut = testPassedStoreMeta(_.put(testStoreObject, pw = 3), _.getPw, new Quorum(3))
    def testPassedDWPut = testPassedStoreMeta(_.put(testStoreObject, dw = 1), _.getDw, new Quorum(1))
    def testPassedReturnBodyPut = testPassedStoreMeta(_.put(testStoreObject, returnBody = true), _.getReturnBody, true)

    val testDate = new Date(System.currentTimeMillis())
    val testVClock = mock[VClock]
  }  
  
  object conflictedFetch extends context {

    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val mock1Bytes = Array[Byte](1, 2)
    val mock1VClockStr = "a vclock"
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, mock1Bytes, testContentType, mock1VClockStr)

    val mock2Bytes = Array[Byte](1, 3)
    val mock2VClockStr = "a vclock2"
    val mockRiakObj2 = mockRiakObj(testBucket, testKey, mock2Bytes, testContentType, mock2VClockStr)


    val multiObjectResponse = mockRiakResponse(Array(mockRiakObj1, mockRiakObj2))

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns multiObjectResponse


    def testDefaultConflictRes = {
      val r = bucket.fetch(testKey).unsafePerformIO()

      r.either must beLeft.like {
        case e => ((_: Throwable) must beAnInstanceOf[UnresolvedConflictException]).forall(e.list)
      }
    }
  }

  object nonEmptyMetadataFetch extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val metadata = Map("m1" -> "v1", "m2" -> "v2")
    val mockObj = mockRiakObj(testBucket, testKey, "value".getBytes, "text/plain", "vclock", metadata = metadata)
    val mockResp = mockRiakResponse(Array(mockObj))
    
    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResp

    lazy val result: Option[ReadObject] = {
      val r: ValidationNel[Throwable, Option[ReadObject]] = bucket.fetch(testKey).unsafePerformIO()

      r.toOption | None
    }

    def testHasCorrectMetadata = {
      result must beSome.which { _.metadata == metadata }
    }
    
    def testHasMetadataIsTrue = {
      result must beSome.which { _.hasMetadata }
    }
    
    def testContainsMetadataForExistingKey = {
      result must beSome.which { o => o.containsMetadata(metadata.keys.head) && o.containsMetadata(metadata.keys.tail.head) }
    }
    
    def testContainsMetadataForMissingKey = {
      result must beSome.which { !_.containsMetadata("dne") }
    }
    
    def testGetMetadataForExistingKey = {
      result must beSome.like {
        case o =>((_:Option[String]) must beSome).forall(o.getMetadata(metadata.keys.head) :: o.getMetadata(metadata.keys.tail.head) :: Nil)
      }
    }

    def testGetMetadataForMissingKey = {
      result must beSome.which {
        !_.getMetadata("dne").isDefined
      }
    }
        
  }

  object nonEmptyIntIndexesFetch extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val indexes = Map("idx1" -> Set(1,2), "idx2" -> Set(3))
    val expectedIndexes = (for { (k,v) <- indexes } yield (IntIndex.named(k), v)).toMap
    val mockObj = mockRiakObj(testBucket, testKey, "value".getBytes, "text/plain", "vlock", intIndexes = indexes)

    val mockResp = mockRiakResponse(Array(mockObj))

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResp

    lazy val result: Option[ReadObject] = {
      bucket.fetch(testKey)
        .map(_.toOption | None)
        .unsafePerformIO()
    }

    def testHasAll = {
      result must beSome.like {
        case obj => obj.intIndexes must haveTheSameElementsAs(expectedIndexes)
      }
    }

    def testGetIntIndexExists = {
      result must beSome.like {
        case obj => obj.intIndex("idx1") must beSome.like {
          case values => values must haveTheSameElementsAs(expectedIndexes(IntIndex.named("idx1")))
        }
      }
    }

    def testGetIntIndexMissing = {
      result must beSome.like {
        case obj => obj.intIndex("dne") must beNone
      }
    }
  }

  object nonEmptyBinIndexesFetch extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val indexes = Map("idx1" -> Set("a", "b"), "idx2" -> Set("d"))
    val expectedIndexes = (for { (k,v) <- indexes } yield (BinIndex.named(k), v)).toMap
    val mockObj = mockRiakObj(testBucket, testKey, "value".getBytes, "text/plain", "vclock", binIndexes = indexes)

    val mockResp = mockRiakResponse(Array(mockObj))

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResp

    lazy val result: Option[ReadObject] = {
      bucket.fetch(testKey)
        .map(_.toOption | None)
        .unsafePerformIO()
    }

    def testHasAll = {
      result must beSome.like {
        case obj => obj.binIndexes must haveTheSameElementsAs(expectedIndexes)
      }
    }

    def testGetBinIndexExists = {
      result must beSome.like {
        case obj => obj.binIndex("idx1") must beSome.like {
          case values => values must haveTheSameElementsAs(expectedIndexes(BinIndex.named("idx1")))
        }
      }
    }

    def testGetBinIndexMissing = {
      result must beSome.like {
        case obj => obj.binIndex("dne") must beNone
      }
    }
  }

  object nonEmptyLinkFetch extends context {
    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val links = (new RiakLink(testBucket, "somekey", "tag")) :: (new RiakLink(testBucket, "somekey", "tag")) :: Nil
    val expectedLinks = links map { l => ScaliakLink(l.getBucket, l.getKey, l.getTag) }
    val mockObj = mockRiakObj(testBucket, testKey, "value".getBytes, "text/plain", "vclock", links)

    val mockResp = mockRiakResponse(Array(mockObj))

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns mockResp

    lazy val result: Option[ReadObject] = {
      val r: ValidationNel[Throwable, Option[ReadObject]] = bucket.fetch(testKey).unsafePerformIO()

      r.toOption | None
    }

    def testConvertedLinks = {
      result must beSome.like {
        case obj => obj.links must beSome.like {
          case links => links.list must haveTheSameElementsAs(expectedLinks)
        }
      }
    }

    def testNumLinks = {
      result must beSome.which { _.numLinks == expectedLinks.length }
    }
    
    def testHasLinks = {
      result must beSome.which { _.hasLinks }
    }

    def testContainsLinkTrueIfContained = {
      result must beSome.which { r => r.containsLink(expectedLinks.head) && r.containsLink(expectedLinks.tail.head) }
    }

    def testContainsLinkFalseIfNotContained = {
      result must beSome.which { !_.containsLink(ScaliakLink("who", "cares", "shouldnt-match")) }
    }

  }

  object simpleFetch extends context {

    val rawClient = mock[RawClientWithStreaming]
    val bucket = createBucket

    val mock1Bytes = Array[Byte](1, 2)
    val mock1VClockStr = "a vclock"
    val mock1VTag = "vtag"
    val mock1LastModified = new java.util.Date(System.currentTimeMillis)
    val mockRiakObj1 = mockRiakObj(testBucket, testKey, mock1Bytes, testContentType, mock1VClockStr, vTag = mock1VTag, lastModified = mock1LastModified)

    val singleObjectResponse = mockRiakResponse(Array(mockRiakObj1))

    rawClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) returns singleObjectResponse

    def someWKey = {
      result must beSome.which { _.key == testKey }
    }

    def testGetBytes = {
      result must beSome.which { _.getBytes == mock1Bytes }
    }

    def testStringValue = {
      result must beSome.which { _.stringValue == new String(mock1Bytes) }
    }

    def testBucketName = {
      result must beSome.which { _.bucket == testBucket }
    }

    def testVClock = {
      result must beSome.which {
        _.vClock.getBytes.toList == mock1VClockStr.getBytes.toList
      }
    }

    def testVClockStr = {
      result must beSome.which { _.vClockString == mock1VClockStr }
    }

    def testVTag = {
      result must beSome.which { _.vTag == mock1VTag }
    }

    def testLastModified = {
      result must beSome.like {
        case obj => obj.lastModified must_== mock1LastModified
      }
    }

    def tContentType = {
      result must beSome.which { _.contentType == testContentType }
    }

    def testEmptyLinksIsNone = {
      result must beSome.like {
        case obj => obj.links must beNone
      }
    }

    def testEmptyLinkHasLinksIsFalse = {
      result must beSome.which { !_.hasLinks }
    }
    
    def testEmptyLinksReturnsZeroNumLinks = {
      result must beSome.which { _.numLinks == 0 }
    }
    
    def testEmptyLinksContainsLinkReturnsFalse = {
      val link1 = ScaliakLink("who", "cares", "at all")
      val link2 = ScaliakLink("because", "this-object", "has-no-links")
      val Some(r) = result
      val res1 = r.containsLink(link1)
      val res2 = r.containsLink(link2)
      List(res1, res2) must haveTheSameElementsAs(false :: false :: Nil)
    }

    def testEmptyMetadataMap = {
      result must beSome.like {
        case obj => obj.metadata must beEmpty
      }
    }
    
    def testEmptyMetadataHasMetadataReturnsFalse = {
      result must beSome.which { !_.hasMetadata }
    }
    
    def testEmptyMetadataContainsMetadataReturnsFalse = {
      val Some(r) = result
      val res1 = r.containsMetadata("m1")
      val res2 = r.containsMetadata("m2")
      List(res1, res2) must haveTheSameElementsAs(false :: false :: Nil)
    }
    
    def testEmptyMetadataGetReturnsNone = {
      val Some(r) = result
      val res1 = r.getMetadata("m1")
      val res2 = r.getMetadata("m2")
      List(res1, res2) must haveTheSameElementsAs(None :: None :: Nil)
    }

    def testEmptyBinIndexes = {
      result must beSome.like {
        case obj => obj.binIndexes must beEmpty
      }
    }

    def testEmptyBinIndexesGetIndexReturnsNone = {
      result must beSome.like {
        case obj => obj.binIndex("whocares") must beNone
      }
    }

    def testEmptyIntIndexes = {
      result must beSome.like {
        case obj => obj.intIndexes must beEmpty
      }
    }

    def testEmptyIntIndexesGetReturnsNone = {
      result must beSome.like {
        case obj => obj.intIndex("whocares") must beNone
      }
    }

    def testConversionExplicit = {
      // this will fail unti you start explicitly passing a resolver
      val r = bucket.fetch(testKey)(dummyDomainConverter, ScaliakResolver.DefaultResolver).unsafePerformIO()

      (r.toOption | None) aka "the optional result discarding the exceptions" must beSome.which {
        _.someField == testKey
      }
    }

    def testConversionImplicit = {
      implicit val converter = dummyDomainConverter
      val r: ValidationNel[Throwable, Option[DummyDomainObject]] = bucket.fetch(testKey).unsafePerformIO()

      (r.toOption | None) aka "the optional result discarding the exceptions" must beSome.which {
        _.someField == testKey
      }
    }
    
    def testDangerous = {
      val newClient = mock[RawClientWithStreaming]
      val newBucket = createBucketWithClient(newClient)
      newClient.fetch(MM.eq(testBucket), MM.eq(testKey), MM.isA(classOf[FetchMeta])) throws (new NullPointerException)
      newBucket.fetchDangerous(testKey).unsafePerformIO() must throwA[NullPointerException]
    }
    
    def testUnsafe = {
      (bucket.fetchUnsafe(testKey).toOption | None) must beSome
    }

    // the result after discarding any possible exceptions
    lazy val result: Option[ReadObject] = {
      val r: ValidationNel[Throwable, Option[ReadObject]] = bucket.fetch(testKey).unsafePerformIO()

      r.toOption | None
    }

  }

  trait context {
    val testBucket = "test_bucket"
    val testKey = "somekey"
    val testContentType = "text/plain"

    def rawClient: RawClientWithStreaming

    def createBucketWithClient(r: RawClientWithStreaming) =  new ScaliakBucket(
      rawClientOrClientPool = Left(r),
      name = testBucket,
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

    def createBucket = createBucketWithClient(rawClient)
  }

}
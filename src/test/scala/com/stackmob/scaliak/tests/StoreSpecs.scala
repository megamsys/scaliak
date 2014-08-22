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
import com.stackmob.scaliak._

import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.links._

import com.basho.riak.client.core.query.functions.Function
import com.basho.riak.client.core.operations.FetchOperation
import com.basho.riak.client.core.operations.StoreOperation
import com.basho.riak.client.core.operations.DeleteOperation
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.client.api.cap._
import com.basho.riak.client.core.query.RiakObject

import scala.collection.JavaConverters._
import com.basho.riak.client.api.cap.{ UnresolvedConflictException, Quorum }
import com.basho.riak.client.core.query.indexes.{ StringBinIndex, LongIntIndex }
import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation
import com.basho.riak.client.api.commands.indexes.{ IntIndexQuery, BinIndexQuery, SecondaryIndexQuery }

import java.util.Date
import scalaz._
import Scalaz._
import scalaz.NonEmptyList._

class StoreSpecs extends RiakWithBucketSpecs {
  def is =
    "Store :".title ^ br ^
      "This class provides the primary functionality for storing data in Riak." ^ br ^
      "Writing Data" ^ br ^
      "With No Conversion" ^ br ^
      "When the Key Being Fetched Does Not Exist" ^ br ^
      """Given the default "Clobber Mutation" """ ^ endp ^
      "Writes the ReadObject as passed in (converted to an RiakObject)" ! writeMissing.performsWrite ^ br ^
      "returns successfully with the stored object as a ReadObject instance" ! writeMissing.returnBody ^ br ^
      p ^
      "When the Key Being Fetched Exists" ^ br ^
      """Given the default "Clobber Mutator" """ ^ endp ^
      "Writes the ReadObject as passed in (converted to an RiakObject)" ! writeExisting.performsWrite ^ br ^
      "Can store the links on an object" ! writeExisting.testStoreUpdateLinks ^ br ^
      "Can put the links on an object" ! writeExisting.testPutUpdateLinks ^ br ^
      end

  class DummyDomainObject(val someField: String)

  val dummyWriteVal = "dummy-howdy"

  val dummyDomainConverter = ScaliakConverter.newConverter[DummyDomainObject](
    o => (new DummyDomainObject(o.key)).successNel[Throwable],
    o => {
      WriteObject(o.someField, dummyWriteVal.getBytes)
    })

  val mutationValueAddition = "abc"

  val dummyDomainMutation = ScaliakMutation.newMutation[DummyDomainObject] {
    (mbOld, newObj) =>
      {
        new DummyDomainObject(newObj.someField + mutationValueAddition)
      }
  }

  object writeMissing extends writeBase {

    def customMutator = {
      var fakeValue: String = "fail"
      implicit val mutator = ScaliakMutation.newMutation[ReadObject] {
        (o: Option[ReadObject], n: ReadObject) =>
          {
            fakeValue = "custom"
            n.copy(bytes = fakeValue.getBytes)
          }
      }
      val newBucket = riak.bucket
      val cusres = newBucket.get.store(testStoreGood1Object).unsafePerformIO().toOption

      cusres.get must beSome.like {
        case obj => obj.stringValue must_== fakeValue
      }
    }

    def domainObject = {
      implicit val converter = dummyDomainConverter
      val newBucket = riak.bucket
      val domres = newBucket.get.store(new DummyDomainObject(riak.testKey1)).unsafePerformIO().toOption

      domres.get must beNone
    }

    def domainObjectCustomMutator = {
      implicit val converter = dummyDomainConverter
      implicit val mutation = dummyDomainMutation
      val newBucket = riak.bucket
      val domcusm = newBucket.get.store(new DummyDomainObject(riak.testKey1)).unsafePerformIO().toOption

      domcusm.get must beNone
    }

    def returnBody = {
      val newBucket = riak.bucket
      val retcusm = newBucket.get.store(testStoreGoodObject, returnBody = true).unsafePerformIO().toOption
      retcusm must beSome.which { obj =>
        obj.get.stringValue == testStoreGoodObject.stringValue
      }
    }
  }

  object writeExisting extends writeBase {
    def testStoreUpdateLinks = {
      val r = strres.get // execute call

      r must beNone
    }

    def testStoreUpdateMetadata = {
      import scala.collection.JavaConverters._
      val r = strres.get // execute call

      r must beSome.like {
        case obj => obj.metadata must beEqualTo(testStoreGoodObject.metadata)
      }
    }

    def testPutUpdateLinks = {
      implicit val converter = dummyDomainConverter

      lazy val putres = {
        bucket.get.put(new DummyDomainObject(riak.testKey)).unsafePerformIO().toOption
      }

      val r = putres.get // execute call
      r must beNone
    }

  }

  trait writeBase {

    val bucket = riak.bucket

    class CustomMutation extends ScaliakMutation[ReadObject] {
      val fakeValue = "custom"
      def apply(old: Option[ReadObject], newObj: ReadObject) = {
        newObj.copy(bytes = fakeValue.getBytes)
      }
    }

    val testStoreBadObject = new ReadObject(
      riak.testKey,
      riak.testBucket,
      riak.testContentType,
      new BasicVClock(""),
      "".getBytes,
      links = nels(ScaliakLink(riak.testBucket, riak.testKey, "invite=invite")).some,
      metadata = Map("m1" -> "v1", "m2" -> "v2"),
      binIndexes = Map("idx1" -> Set("act001"), "idx2" -> Set("nod001")),
      intIndexes = Map("idx1" -> Set(1L), "idx2" -> Set(3L)))

    val testStoreGoodObject = new ReadObject(
      riak.testKey,
      riak.testBucket,
      riak.testContentType,
      new BasicVClock("vclock"),
      "hey there 1".getBytes,
      links = nels(ScaliakLink(riak.testBucket, riak.testKey, "invite")).some,
      metadata = Map("m1" -> "v1", "m2" -> "v2"),
      binIndexes = Map("idx1" -> Set("act0002"), "idx2" -> Set("nod002")),
      intIndexes = Map("idx1" -> Set(1L), "idx2" -> Set(3L)))

    val testStoreGood1Object = new ReadObject(
      riak.testKey1,
      riak.testBucket,
      riak.testContentType,
      new BasicVClock("vclock"),
      "hey there 2".getBytes,
      links = nels(ScaliakLink(riak.testBucket, riak.testKey, "follow")).some,
      metadata = Map("m1" -> "v1", "m2" -> "v2"),
      binIndexes = Map("idx1" -> Set("act0001"), "idx2" -> Set("nod002")),
      intIndexes = Map("idx1" -> Set(1L), "idx2" -> Set(3L)))

    lazy val strres1 = bucket.get.store(testStoreGoodObject).unsafePerformIO()

    val strres = strres1.toOption

    def performsWrite = {
      strres must beSome
    }

  }

}

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

// TODO: these specs really cover both ReadObject and ScaliakBucket, they should be split up
class FetchSpecs extends RiakWithBucketSpecs {

  def is =
    "Fetch :".title ^ br ^
      "Functionality for fetching data  from in Riak." ^ br ^
      "Fetching Data" ^ br ^
      "Fetching with No Conversion" ^ br ^
      "When the key being fetched exists" ^ br ^
      "When there are no conflicts" ^ br ^
      "returns a ReadObject whose key is the same as the one fetched" ! simpleFetch.someWKey ^ br ^
      "can get the stored bytes by calling getBytes on the returned object" ! simpleFetch.testGetBytes ^ br ^
      "calling stringValue on the returned object returns the string value" ! simpleFetch.testStringValue ^ br ^
      "the returned object has the same bucket name as the one used to fetch it" ! simpleFetch.testBucketName ^ br ^
      // "the returned object has a vclock" ! simpleFetch.testVClock ^ br ^
      // "calling vclockString returns the vclock as a string" ! simpleFetch.testVClockStr ^ br ^
      //"the returned object has a vTag" ! simpleFetch.testVTag ^ br ^
      //"the returned object has a lastModified timestamp" ! simpleFetch.testLastModified ^ br ^
      "the returned object has a content type" ! simpleFetch.tContentType ^ br ^
      "if the fetched object has an empty list of links" ^ br ^
      "links returns None" ! simpleFetch.testLinksIsSome ^ br ^
      "hasLinks returns false" ! simpleFetch.testLinkHasLinksIsTrue ^ br ^
      "numLinks returns 0" ! simpleFetch.testLinksReturnsNumLinksSize ^ br ^
      "containsLink returns true for any link" ! simpleFetch.testLinksContainsLinkReturnsTrue ^ p ^
      "if the fetched object has a non-empty list of links" ^
      end

  object simpleFetch {

    val bucket = riak.bucket

    val mockStringVal = "hey there 1"
    val mock1VClockStr = (new BasicVClock("good_vclock")).asString
    val mock1VTag = "vtag"
    val mock1LastModified = new java.util.Date(System.currentTimeMillis)

    def someWKey = {
      result must beSome.which { _.key == riak.testKey }
    }

    def testGetBytes = {
      result must beSome.which { x => ((new String(x.getBytes)) == mockStringVal) }
    }

    def testStringValue = {
      result must beSome.which { _.stringValue == mockStringVal }
    }

    def testBucketName = {
      result must beSome.which { _.bucket == riak.testBucket }
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
      result must beSome.which { _.contentType == riak.testContentType }
    }

    def testLinksContainsLinkReturnsTrue = {
      val link1 = ScaliakLink(riak.testBucket, riak.testKey, "invite")
      val Some(r) = result
      val res1 = r.containsLink(link1)
      List(res1) must contain(exactly(List(true): _*))
    }

    def testLinksIsSome = {
      result must beSome.like {
        case obj => obj.links must beSome
      }
    }

    def testLinkHasLinksIsTrue = {
      result must beSome.which { _.hasLinks }
    }

    def testLinksReturnsNumLinksSize = {
      result must beSome.which { _.numLinks > 0 }
    }



    def testEmptyMetadataMap = {
      result must beSome.like {
        case obj => obj.metadata must beEmpty
      }
    }

    def testEmptyMetadataHasMetadataReturnsFalse = {
      result must beSome.which { !_.hasMetadata }
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

    lazy val result: Option[ReadObject] = {
      val r: ValidationNel[Throwable, Option[ReadObject]] = riak.bucket match {
        case Some(b) => b.fetch(riak.testKey).unsafePerformIO()
        //case None => println("Nada!")
      }
      r.toOption.get
    }

  }

}

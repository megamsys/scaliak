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
import com.basho.riak.client.api.cap.VClock
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

class FetchIndexSpecs extends RiakWithBucketSpecs {
  def is =
    "Fetch Index".title ^
      """
  This class provides the primary functionality for fetching data
  from and storing data in Riak.
  """ ^
      p ^
      endp ^
      "Fetching by Index" ^ br ^
      "by value" ^ br ^
      "if the value is a string a BinValueQuery is performed" ! fetchIndexByValue.testBinValueGeneratesBinQuery ^ br ^
      "if the value is an integer a IntValueQuery is performed" ! fetchIndexByValue.testIntValueGeneratesIntQuery ^ br ^
      //"returns the results of the query, a List[String] as returned by the client" ! fetchIndexByValue.testReturnsKeys ^ p ^
      end

  object fetchIndexByValue  {

    val testIdx = "idx1"

    def testBinValueGeneratesBinQuery = {
      val b = riak.bucket
      val testBinVal = "act0002"

      val testResults: java.util.List[String] = new java.util.LinkedList[String]()
      testResults.add("idx1")

      val res = b.get.fetchIndexByValue(index = testIdx, value = testBinVal).unsafePerformIO().toOption // execute

      res must beSome.which(_.length > 0)
    }

    def testIntValueGeneratesIntQuery = {
      val b = riak.bucket
      val testIntVal = 1

      val res = b.get.fetchIndexByValue(index = testIdx, value = testIntVal).unsafePerformIO().toOption // execute
      res must beSome.which(_.length > 0)

    }

    def testReturnsKeys = {
      import scala.collection.JavaConverters._
      val b = riak.bucket
      val indexVal = "act002"
      val testResults: java.util.List[String] = new java.util.LinkedList[String]()
      testResults.add("1")
      testResults.add("2")

      b.get.fetchIndexByValue(index = testIdx, value = indexVal).map(_.toOption).unsafePerformIO() must beSome.like {
        case res => res must beEqualTo(testResults.asScala.toList)
      }
    }

  }

  

}

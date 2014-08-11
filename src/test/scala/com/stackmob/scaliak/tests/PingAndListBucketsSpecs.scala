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
import scala.util.control.Exception._
import java.nio.charset.Charset

import java.util.Date
import scalaz._
import Scalaz._
import scalaz.NonEmptyList._

class PingAndListBucketsSpecs extends RiakSpecs {

  def is =
    "ListBuckets & Ping :".title ^ br ^
      "Functionality for listing buckets, and pinging Riak." ^ br ^
      "List Buckets" ^ br ^
      "Ping Riak" ^ br ^
      "can do a ping to riak" ! ping.testPing ^ br ^
      "can get the list of buckets" ! listBuckets.testList ^ br ^
      "can figure out charset UTF8" ! charSet.testCharset1 ^ br ^
      "can figure out invalid charset" ! charSet.testCharset2 ^ br ^
      "can figure out charset ISO8859" ! charSet.testCharset3 ^ br ^
      endp ^
      end

  object ping {
    val result = riak.client.pb.ping

    def testPing = {
      result must haveClass[scala.runtime.BoxedUnit]
    }
  }

  object listBuckets {
    val result = riak.client.listBuckets.unsafePerformIO().toOption

    def testList = {
      result must beSome.which { _.contains("test_bucket") }
    }
  }

  object charSet {
    val cdata1 = Some("""<meta http-equiv="content-type" content="text/html; charset=UTF-8">""")
    val invalid_data1 =Some("""<meta http-equiv="content-type" content="text/html; charset=UTF-8A">""")
    val cdata3 = None

    private lazy val charsetMatcher = """.*;\s*charset=([^\(\)<>@,;:\\"/\[\]\?={}\s\t]+);?\s*.*$""".r

    private def getCharset(contentType: Option[String]): Charset = (contentType.getOrElse(new String()) match {
      case charsetMatcher(charset) => catching(classOf[Exception]).opt(java.nio.charset.Charset.forName(charset))
      case _                       => None
    }).getOrElse(Charset.forName("ISO-8859-1"))

    def testCharset1 = {
      getCharset(cdata1).name() must beEqualTo("UTF-8")
    }

    def testCharset2 = {
      getCharset(invalid_data1).name() must beEqualTo("ISO-8859-1")
    }

    def testCharset3 = {
      getCharset(cdata3).name() must beEqualTo("ISO-8859-1")
    }
  }

}

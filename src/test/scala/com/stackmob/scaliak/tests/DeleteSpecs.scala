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
/**
 * @author ram
 *
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

class DeleteSpecs extends RiakWithBucketSpecs {
  def is =
    "Delete".title ^
      """
  This class provides the primary functionality for deleting data
  from Riak 2.0.
  """ ^
      p ^
      endp ^
  "Deleting Data" ^ br ^
    "By Key" ^ br ^
    "Uses the raw client passing in the bucket name, key and delete meta" ! deleteByKey.test ^ p ^
    "By ReadObject" ^ br ^
    "Deletes the object by its key" ! deleteScaliakObject.test ^ p ^
    "By Domain Object" ^ br ^
    "Deletes the object by its key" ! deleteDomainObject.test ^ p ^
    endp ^
    end

  class DummyDomainObject(val someField: String)
  val dummyWriteVal = "dummy"
  val dummyDomainConverter = ScaliakConverter.newConverter[DummyDomainObject](
    o => (new DummyDomainObject(o.key)).successNel[Throwable],
    o => WriteObject(o.someField, dummyWriteVal.getBytes),
    o => o.someField)
  val mutationValueAddition = "abc"
  val dummyDomainMutation = ScaliakMutation.newMutation[DummyDomainObject] {
    (mbOld, newObj) =>
      {
        new DummyDomainObject(newObj.someField + mutationValueAddition)
      }
  }

  object deleteDomainObject  {
    val bucket = riak.bucket.get

    val obj = new DummyDomainObject(riak.testKey)
    implicit val converter = dummyDomainConverter
    lazy val result = bucket.delete(obj).unsafePerformIO().toOption

    def test = {
      result must beSome.like {
        case obj => obj must haveClass[scala.runtime.BoxedUnit]
      }
    }
  }

  object deleteScaliakObject {
    val bucket = riak.bucket.get

    val obj = ReadObject(riak.testKey, riak.testBucket, riak.testContentType, new BasicVClock(""), "".getBytes)
    lazy val result = bucket.delete(obj).unsafePerformIO().toOption

    def test = {
      result must beSome.like {
        case obj => obj must haveClass[scala.runtime.BoxedUnit]
      }
    }
  }

  object deleteByKey  {
    val bucket = riak.bucket.get

    lazy val result = bucket.deleteByKey(riak.testKey).unsafePerformIO().toOption

    def test = {
      result must beSome.like {
        case obj => obj must haveClass[scala.runtime.BoxedUnit]
      }
    }
  }

 
}

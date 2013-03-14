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

package com.stackmob.scaliak.tests.util

import com.basho.riak.client.cap.VClock
import com.basho.riak.client.raw.RiakResponse
import org.specs2._
import mock._
import com.basho.riak.client.{RiakLink, IRiakObject}
import scala.collection.JavaConverters._
import com.basho.riak.client.query.indexes.{IntIndex, BinIndex}

trait MockRiakUtils {
  this: Specification with Mockito =>

  def mockRiakObj(bucket: String,
                  key: String,
                  value: Array[Byte],
                  contentType: String,
                  vClockStr: String,
                  links: List[RiakLink] = List(),
                  metadata: Map[String, String] = Map(),
                  vTag: String = "",
                  lastModified: java.util.Date = new java.util.Date(System.currentTimeMillis),
                  binIndexes: Map[String, Set[String]] = Map(),
                  intIndexes: Map[String, Set[Int]] = Map()): IRiakObject = {

    val mocked = mock[IRiakObject]
    val mockedVClock = mock[VClock]
    mockedVClock.asString returns vClockStr
    mockedVClock.getBytes returns vClockStr.getBytes
    mocked.getKey returns key
    mocked.getValue returns value
    mocked.getBucket returns bucket
    mocked.getVClock returns mockedVClock
    mocked.getContentType returns contentType    
    mocked.getLinks returns links.asJava
    mocked.getVtag returns vTag
    mocked.getLastModified returns lastModified
    mocked.getMeta returns metadata.asJava
    mocked.allBinIndexes returns ((for { (k,v) <- binIndexes } yield (BinIndex.named(k), v.asJava)).toMap.asJava)
    mocked.allIntIndexesV2 returns ((for { (k,v) <- intIndexes } yield (IntIndex.named(k), v.map(new java.lang.Long(_)).asJava)).toMap.asJava)

    mocked
  }

  def mockRiakResponse(objects: Array[IRiakObject]) = {
    val mocked = mock[RiakResponse]
    mocked.getRiakObjects returns objects
    mocked.numberOfValues returns objects.length

    mocked
  }

}
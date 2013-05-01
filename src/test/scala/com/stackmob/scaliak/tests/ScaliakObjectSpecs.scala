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
import scalaz._
import scalaz.NonEmptyList._
import Scalaz._
import com.stackmob.scaliak.{ScaliakLink, ReadObject}

class ScaliakObjectSpecs extends Specification { def is =
  "Scaliak Object".title                                                            ^
  """
  Convenience methods for working with ReadObject (if not using domain objects),
  specifically metadata & links
  """                                                                               ^
                                                                                    p^
  "Updating Metadata"                                                               ^
    "Adding a Single Metadata Value"                                                ^
      "can add the pair passing in a key and string"                                ! testAddKeyVal ^
      "can add the pair passing in a key, string pair"                              ! testAddKeyValPair ^p^
    "Can merge a map into the existing metadata"                                    ! testMerge ^
    "Can remove an existing key from the metadata map"                              ! removeMeta ^
                                                                                    endp^
  "Updating Links"                                                                  ^
    "Adding Links"                                                                  ^
      "Can pass in a ScaliakLink"                                                   ! testAddSingleLink1 ^
      "Can pass in a sequence of ScaliakLink instances"                             ! testAddLinksSequence ^
      "Can pass in a bucket, key, and tag"                                          ! testAddSingleLink2 ^p^
    "Removing Links"                                                                ^
      "Can remove a single link passing in an instance of ScaliakLink"              ! testRemoveLinkInstance ^
      "Can remove a single link passing in a bucket, key, and tag"                  ! testRemoveLinkByBucketKeyTag ^
      "Can remove links by just passing in a tag"                                   ! testRemoveLinksByTag ^
                                                                                    end

  def testAddKeyVal = {
    testObject.addMetadata("a", "b").metadata must havePair("a" -> "b")
  }

  def testAddKeyValPair = {
    testObject.addMetadata("a" -> "b").metadata must havePair("a" ->"b")
  }

  def testMerge = {
    testObject.mergeMetadata(Map(existingMetadataKey -> "newValue", "m2" -> "v2")).metadata must
      havePair(existingMetadataKey -> "newValue") and havePair("m2" -> "v2")
  }

  def removeMeta = {
    testObject.removeMetadata(existingMetadataKey).metadata must beEmpty
  }

  def testAddSingleLink1 = {
    val newLink = ScaliakLink("bucket", "key3", "tag")
    val newLinks = ~((testObject addLink newLink).links map { _.list })
    val existingLinks = ~(testObject.links map { _.list })
    newLinks must haveTheSameElementsAs(newLink :: existingLinks)
  }

  def testAddSingleLink2 = {
    val newLinks = ~((testObject.addLink("bucket", "key3", "tag")).links map { _.list })
    val existingLinks = ~(testObject.links map { _.list })
    newLinks must haveTheSameElementsAs(ScaliakLink("bucket", "key3", "tag") :: existingLinks)
  }

  def testAddLinksSequence = {
    val newScLinks = ScaliakLink("bucket", "key3", "tag") :: ScaliakLink("bucket", "key4", "tag") :: Nil
    val newLinks = ~((testObject.addLinks(newScLinks)).links map { _.list })
    val existingLinks = ~(testObject.links map { _.list })
    newLinks must haveTheSameElementsAs(newScLinks ++ existingLinks)
  }

  def testRemoveLinkInstance = {
    val newObj = testObject removeLink ScaliakLink("bucket", "key1", "tag")
    ~(newObj.links map { _.list }) must haveSize(~(testObject.links map { _.count }) - 1)
  }

  def testRemoveLinkByBucketKeyTag = {
    val newObj = testObject.removeLink("bucket", "key1", "tag")
    ~(newObj.links map { _.list }) must haveSize(~(testObject.links map { _.count }) - 1)
  }

  def testRemoveLinksByTag = {
    val newObj = testObject removeLinks "tag"
    newObj.links must beNone
  }

  val existingMetadataKey = "m1"
  val existingMetadataVal = "v1"
  val testObject = ReadObject(
    "key", 
    "bucket", "text/plain", 
    null, "".getBytes, 
    metadata = Map(existingMetadataKey -> existingMetadataVal),
    links = nels(ScaliakLink("bucket", "key1", "tag"), ScaliakLink("bucket", "key2", "tag")).some
  )

}
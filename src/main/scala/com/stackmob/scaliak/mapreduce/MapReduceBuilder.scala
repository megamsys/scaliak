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

package com.stackmob.scaliak.mapreduce

import org.json._

object MapReduceBuilder {

  def buildInputs(mrJob: MapReduceJob) = {
    if (mrJob.searchQuery.isDefined) {
      if (!mrJob.bucket.isDefined) throw new RuntimeException("A search query needs a bucket to be set.")
      val jobInputs = new JSONObject
      jobInputs.put("module", "riak_search")
      jobInputs.put("function", "mapred_search")
      jobInputs.put("arg", List(mrJob.bucket.get, mrJob.searchQuery.get))
      jobInputs
    } 
    
    else if (mrJob.bucket.isDefined) {
      mrJob.bucket.get
    } 
    
    else if (mrJob.binIndex.isDefined) {
      val jobInputs = new JSONObject
      jobInputs.put("bucket", mrJob.binIndex.get.bucket)
      jobInputs.put("index", mrJob.binIndex.get.idx + "_bin")
      jobInputs.put("key", mrJob.binIndex.get.idv)
      jobInputs
    } 
    
    else if (mrJob.intIndex.isDefined) {
      val jobInputs = new JSONObject 
      jobInputs.put("bucket", mrJob.intIndex.get.bucket)
      jobInputs.put("index", mrJob.intIndex.get.idx + "_int")
      mrJob.intIndex.get.idv match {
        case Left(int) => jobInputs.put("key", int)
        case Right(range) => {
          jobInputs.put("start", range.start)
          jobInputs.put("end", range.end)
        }
      }
      jobInputs
    } 
    
    else if (mrJob.riakObjects.isDefined) {
      val tupleList = mrJob.riakObjects.get.map { bucketToObjects =>
        bucketToObjects._2.map(obj => new JSONArray(Array(bucketToObjects._1, obj)))
      }
      new JSONArray(tupleList.flatten.toArray)
    } 
    
    else {
      throw new RuntimeException("No inputs found")
    }
  }

  def buildPhase(phase: MapOrReducePhase) = {
    val phaseJson = new JSONObject

    phase.method match {
      case x:MapMethod => {
        val functionJson = phase.fn.toJson
        functionJson.put("keep", phase.keep)
        if (phase.arguments.isDefined) functionJson.put("arg", phase.arguments.get)
        phaseJson.put("map", functionJson)
      }
      case x:ReduceMethod => {
        val functionJson = phase.fn.toJson
        functionJson.put("keep", phase.keep)
        if (phase.arguments.isDefined) functionJson.put("arg", phase.arguments.get)
        phaseJson.put("reduce", functionJson)
      }
    }
    phaseJson
  }

  def toJSON(mrJob: MapReduceJob) = {
    val job = new JSONObject
    job.put("inputs", buildInputs(mrJob))
    val phases = for (phase <- mrJob.mapReducePhasePipe.phases) yield {
      buildPhase(phase)
    }

    val query = new JSONArray(phases.list.toArray)
    job.put("query", query)
    job.put("timeout", mrJob.timeOut)
    // TODO: Add Support for links and time-outs 
    job
  }
}
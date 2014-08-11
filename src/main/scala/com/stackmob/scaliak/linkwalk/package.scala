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

package com.stackmob.scaliak

//import com.basho.riak.client.api.commands.mapreduce.{LinkPhase => JLinkWalkStep}
import scalaz.NonEmptyList
import java.util.LinkedList

package object linkwalk {
  
  /*type LinkWalkSteps = NonEmptyList[LinkWalkStep]
  
  implicit def linkWalkStepToJLinkWalkStep(lws: LinkWalkStep): JLinkWalkStep = {
    new JLinkWalkStep(lws.bucket, lws.tag, lws.keep)
  }
  
  implicit def linkWalkStepToSteps(lws: LinkWalkStep): LinkWalkSteps = NonEmptyList(lws)
  
  implicit def tuple2ToLinkWalkStepTuple2(tpl: (String, String)): LinkWalkStepTuple2 = new LinkWalkStepTuple2(tpl)

  implicit def tuple2ToLinkWalkStep(tpl: (String, String)): LinkWalkStep = tpl.toLinkWalkStep
  
  implicit def tuple3ToLinkWalkStepTuple3(tpl: (String, String, Boolean)): LinkWalkStepTuple3 = new LinkWalkStepTuple3(tpl)
  
  implicit def tuple3ToLinkWalkStep(tpl: (String, String, Boolean)): LinkWalkStep = tpl.toLinkWalkStep
  
  implicit def nelLwsToLinkWalkStepsW(ls: NonEmptyList[LinkWalkStep]): LinkWalkStepsW = new LinkWalkStepsW(ls)
  
  implicit def bucketObjTupleToLWTuple(tpl: (ScaliakBucket,  ReadObject)): LinkWalkStartTuple = new LinkWalkStartTuple(tpl)
  
  implicit def linkWalkStepsToJava(steps: LinkWalkSteps): LinkedList[JLinkWalkStep] = {
    val list = new LinkedList[JLinkWalkStep]()
    steps.list foreach { list.add(_) }
    list
  }
  */
}

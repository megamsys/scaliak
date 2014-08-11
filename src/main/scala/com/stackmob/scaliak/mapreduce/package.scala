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
package com.stackmob.scaliak

import scalaz.NonEmptyList
import com.basho.riak.client.core.query.functions.Function

package object mapreduce {
  type MapOrReducePhases = NonEmptyList[MapOrReducePhase]

  implicit def MapOrReducePhaseToMapReducePhases(morp: MapOrReducePhase): MapOrReducePhases = NonEmptyList(morp)

  implicit def tuple4ToMapOrReducePhaseTuple4(tpl: (MapReduceMethod, Function, Boolean, Option[java.lang.Object])): MapOrReducePhaseTuple4 = new MapOrReducePhaseTuple4(tpl)

  implicit def tuple4ToMapOrReducePhase(tpl: (MapReduceMethod, Function, Boolean, Option[java.lang.Object])): MapOrReducePhase = tpl.toMapOrReducePhase

  implicit def nelLwsToMapOrReducePhasesW(ls: NonEmptyList[MapOrReducePhase]): MapOrReducePhasesW = new MapOrReducePhasesW(ls)
}
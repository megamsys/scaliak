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

import scalaz._
import Scalaz._
import com.basho.riak.client.core.query.functions.Function

case class BinaryIndex(idx: String, idv: String, bucket: String)

case class IntegerIndex(idx: String, idv: Either[Int, Range], bucket: String)

case class MapReduceJob(mapReducePhasePipe: MapReducePhasePipe,
                        searchQuery: Option[String] = None,
                        bucket: Option[String] = None,
                        riakObjects: Option[Map[String, Set[String]]] = None,
                        binIndex: Option[BinaryIndex] = None,
                        intIndex: Option[IntegerIndex] = None,
                        timeOut: Int = 60000)

sealed trait MapReducePhasePipe {
  def phases: MapOrReducePhases
}

object MapReducePhasePipe {
  def apply(p: MapOrReducePhases): MapReducePhasePipe = new MapReducePhasePipe {
    override lazy val phases = p
  }

  def apply(p: MapOrReducePhase): MapReducePhasePipe = apply(p.wrapNel)
}

trait MapReduceMethod
case class ReduceMethod() extends MapReduceMethod
case class MapMethod() extends MapReduceMethod

sealed trait MapOrReducePhase extends MapReducePhaseOperators {
  def method: MapReduceMethod

  def fn: Function

  def keep: Boolean

  def arguments: Option[java.lang.Object]

  val existingPhases = this.wrapNel.asInstanceOf[MapOrReducePhases]

  override def toString = List(method, fn, keep, arguments).mkString("MapReducePhase(", ",", ")")
}

object MapOrReducePhase {
  def apply(m: MapReduceMethod, f: Function, k: Boolean = false, a: Option[java.lang.Object] = None): MapOrReducePhase = new MapOrReducePhase {
    val method = m
    val fn = f
    val keep = k
    val arguments = a
  }
}

object MapPhase {
  def apply(f: Function, k: Boolean = false, a: Option[java.lang.Object] = None): MapOrReducePhase = new MapOrReducePhase {
    val method = MapMethod()
    val fn = f
    val keep = k
    val arguments = a
  }
}

object ReducePhase {
  def apply(f: Function, k: Boolean = false, a: Option[java.lang.Object] = None): MapOrReducePhase = new MapOrReducePhase {
    val method = ReduceMethod()
    val fn = f
    val keep = k
    val arguments = a
  }
}

sealed trait MapReducePhaseOperators {
  def existingPhases: MapOrReducePhases

  // Using |* and |- only as better options for code markup
  def |*(next: MapOrReducePhase): MapOrReducePhases = add(next)

  def |-(next: MapOrReducePhase): MapOrReducePhases = add(next)

  def |*(nexts: MapOrReducePhases): MapOrReducePhases = add(nexts)

  def |-(nexts: MapOrReducePhases): MapOrReducePhases = add(nexts)

  def add(next: MapOrReducePhase): MapOrReducePhases = add(next.wrapNel)

  def add(nexts: MapOrReducePhases): MapOrReducePhases = {
    existingPhases |+| nexts
  }
}

class MapOrReducePhaseTuple4(value: (MapReduceMethod, Function, Boolean, Option[java.lang.Object])) {
  def toMapOrReducePhase = MapOrReducePhase(value._1, value._2, value._3, value._4)
}

class MapOrReducePhasesW(values: MapOrReducePhases) extends MapReducePhaseOperators {
  val existingPhases = values
}

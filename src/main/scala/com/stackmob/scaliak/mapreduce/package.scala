package com.stackmob.scaliak

import com.basho.riak.client.query.{LinkWalkStep => JLinkWalkStep}
import scalaz.NonEmptyList
import java.util.LinkedList
import scala.collection.mutable.MutableList
import com.basho.riak.pbc.mapreduce.JavascriptFunction

/**

 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */

package object mapreduce {
  type MapOrReducePhases = NonEmptyList[MapOrReducePhase]

  implicit def MapOrReducePhaseToMapReducePhases(morp: MapOrReducePhase): MapOrReducePhases = NonEmptyList(morp)

  implicit def tuple4ToMapOrReducePhaseTuple4(tpl: (MapReduceMethod, JavascriptFunction, Boolean, Option[java.lang.Object])): MapOrReducePhaseTuple4 = new MapOrReducePhaseTuple4(tpl)

  implicit def tuple4ToMapOrReducePhase(tpl: (MapReduceMethod, JavascriptFunction, Boolean, Option[java.lang.Object])): MapOrReducePhase = tpl.toMapOrReducePhase

  implicit def nelLwsToMapOrReducePhasesW(ls: NonEmptyList[MapOrReducePhase]): MapOrReducePhasesW = new MapOrReducePhasesW(ls)
}

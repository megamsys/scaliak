package com.stackmob.scaliak.mapreduce

import scalaz._
import Scalaz._
import com.basho.riak.client.raw._
import com.basho.riak.client.query.{MapReduce, BucketKeyMapReduce, BucketMapReduce}
import com.basho.riak.pbc.mapreduce.JavascriptFunction

import com.stackmob.scaliak.{ScaliakConverter, ScaliakResolver, ReadObject, ScaliakBucket}
import scala.collection.mutable.MutableList
import com.basho.riak.client.query.LinkWalkStep.Accumulate


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

  def apply(p: MapOrReducePhase): MapReducePhasePipe = apply(p.wrapNel.asInstanceOf[MapOrReducePhases])
}

trait MapReduceMethod
case class ReduceMethod() extends MapReduceMethod
case class MapMethod() extends MapReduceMethod

sealed trait MapOrReducePhase extends MapReducePhaseOperators {
  def method: MapReduceMethod

  def fn: JavascriptFunction

  def keep: Boolean

  def arguments: Option[java.lang.Object]

  val existingPhases = this.wrapNel.asInstanceOf[MapOrReducePhases]

  override def toString = List(method, fn, keep, arguments).mkString("MapReducePhase(", ",", ")")
}

object MapOrReducePhase {
  def apply(m: MapReduceMethod, f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): MapOrReducePhase = new MapOrReducePhase {
    val method = m
    val fn = f
    val keep = k
    val arguments = a
  }
}

object MapPhase {
  def apply(f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): MapOrReducePhase = new MapOrReducePhase {
    val method = MapMethod()
    val fn = f
    val keep = k
    val arguments = a
  }
}

object ReducePhase {
  def apply(f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): MapOrReducePhase = new MapOrReducePhase {
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

class MapOrReducePhaseTuple4(value: (MapReduceMethod, JavascriptFunction, Boolean, Option[java.lang.Object])) {
  def toMapOrReducePhase = MapOrReducePhase(value._1, value._2, value._3, value._4)
}

class MapOrReducePhasesW(values: MapOrReducePhases) extends MapReducePhaseOperators {
  val existingPhases = values
}
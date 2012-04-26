package com.stackmob.scaliak.mapreduce

import scalaz._
import Scalaz._
import com.basho.riak.client.raw._
import com.basho.riak.client.query.{ MapReduce, BucketKeyMapReduce, BucketMapReduce }
import com.basho.riak.pbc.mapreduce.JavascriptFunction

import com.stackmob.scaliak.{ ScaliakConverter, ScaliakResolver, ReadObject, ScaliakBucket }
import scala.collection.mutable.MutableList

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */

case class BinaryIndex(idx: String, idv: String, bucket: String)
case class IntegerIndex(idx: String, idv: Either[Int, Range], bucket: String)

case class MapReduceJob(
  val mapReducePhasePipe: MapReducePhasePipe,
  val searchQuery: Option[String] = None,
  val bucket: Option[String] = None,
  val riakObjects: Option[Map[String, Set[String]]] = None,
  val binIndex: Option[BinaryIndex] = None,
  val intIndex: Option[IntegerIndex] = None,
  val timeOut: Int = 60000)

sealed trait MapReducePhasePipe {
  val phases: MapReducePhases
}

object MapReducePhasePipe {

  def apply(p: MapReducePhases): MapReducePhasePipe = new MapReducePhasePipe {
    val phases = p
  }

  def apply(p: MapPhase): MapReducePhasePipe = new MapReducePhasePipe {
    val phases = MutableList[Either[MapPhase, ReducePhase]](Left(p))
  }

  def apply(p: ReducePhase): MapReducePhasePipe = new MapReducePhasePipe {
    val phases = MutableList[Either[MapPhase, ReducePhase]](Right(p))
  }
}

sealed trait MapReducePhase {
  val existingPhases = MutableList[Either[MapPhase, ReducePhase]]()

  def |*(next: MapPhase): MapReducePhases = add(Left(next))
  def |-(next: ReducePhase): MapReducePhases = add(Right(next))
  def >>(nexts: MapReducePhases): MapReducePhases = add(nexts)

  def add(next: MapOrReducePhase): MapReducePhases = add(MutableList[Either[MapPhase, ReducePhase]](next))
  def add(nexts: MapReducePhases): MapReducePhases = {
    existingPhases |+| nexts
  }
}

sealed trait MapPhase extends MapReducePhase {

  def fn: JavascriptFunction
  def keep: Boolean
  def arguments: Option[java.lang.Object]
}

object MapPhase {

  def apply(f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): MapPhase = {
  val phase = new MapPhase {
      val fn = f
      val keep = k
      val arguments = a
    }
    phase.existingPhases += Left(phase)
    phase
  }
}

sealed trait ReducePhase extends MapReducePhase {
  def fn: JavascriptFunction
  def keep: Boolean
  def arguments: Option[java.lang.Object]
}

object ReducePhase {
  def apply(f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): ReducePhase = {
    val phase = new ReducePhase {
      val fn = f
      val keep = k
      val arguments = a
    }
    phase.existingPhases += Right(phase)
    phase
  }
}
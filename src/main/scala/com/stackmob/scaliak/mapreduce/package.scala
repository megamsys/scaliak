package com.stackmob.scaliak

import com.basho.riak.client.query.{LinkWalkStep => JLinkWalkStep}
import scalaz.NonEmptyList
import java.util.LinkedList
import scala.collection.mutable.MutableList
/**

 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */

package object mapreduce {
  type MapReducePhases = MutableList[MapOrReducePhase]
  type MapOrReducePhase = Either[MapPhase, ReducePhase]
}

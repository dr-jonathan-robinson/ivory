package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class DaysInReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Days in reducer works for arbitrary string values                  $daysIn
  Days in reducer works for arbitrary string values with tombstones  $daysInWithTombtones
  Days in reducer laws                                               $daysInLaws
"""

  def daysIn = prop((facts: ValuesWithDate[String]) => {
    // So we can observe the "last" value in the list, we add an additional dummy to the end,
    // which is placed at the end of the time window. This gives us the last value with time up
    // until the end of the window.
    val valsWithDummyEnd = facts.ds ++ List( ("", facts.offsets.end ) )

    val expected =
        valsWithDummyEnd.sliding(2).collect {
        case (av,ad) :: (_,bd) :: Nil =>
          (av, DateTimeUtil.toDays(bd) - DateTimeUtil.toDays(ad))
        }
        .toList
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)

    ReducerUtil.runWithDates(new DaysInReducer[String](facts.offsets, ""), facts.ds).map.asScala.toMap ==== expected
  })

def daysInWithTombtones = prop((facts: OptValuesWithDate[String]) => {
    val valsWithDummyEnd = facts.ds ++ List( ("", facts.offsets.end ) )

    val expected =
        valsWithDummyEnd.sliding(2).collect {
        case (av,ad) :: (_,bd) :: Nil =>
          (av, DateTimeUtil.toDays(bd) - DateTimeUtil.toDays(ad))
        }
        .toList
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
        .collect { case (Some(k : String), v) => k -> v }
        .toMap

    ReducerUtil.runWithDatesAndTombstones(new DaysInReducer[String](facts.offsets, ""), facts.ds).map.asScala.toMap ==== expected
  })

  def daysInLaws =
    ReducerUtil.reductionFoldWithDateLaws(offsets => new DaysInReducer[Int](offsets, 0))
}

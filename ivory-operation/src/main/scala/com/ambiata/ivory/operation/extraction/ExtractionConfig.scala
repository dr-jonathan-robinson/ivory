package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.control._

import scalaz._, Scalaz._, \&/._

case class ExtractionConfig(groupByEntityReducers: Option[Int])

object ExtractionConfig {

  val GroupByEntityNumReducers = "ivory.extraction.GroupByEntity.reducers"

  def fromProperties: RIO[ExtractionConfig] = for {
    prop     <- RIO.io(Option(System.getProperty(GroupByEntityNumReducers)))
    reducers <- prop.traverse(p => RIO.fromDisjunction(p.parseInt.disjunction.leftMap(t => Both(s"Bad value for property '${GroupByEntityNumReducers}'", t))))
  } yield ExtractionConfig(reducers)
}

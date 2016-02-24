package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._, arbitraries._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.ivory.storage.statistics._

import org.specs2._
import org.specs2.execute.{Result, AsResult}

import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.Hdfs

import scalaz._, Scalaz._

class FactStatsJobSpec extends Specification with ScalaCheck { def is = s2"""

FactStatsJob
------------

  mr job produces correct statistics                                 $mrStats    ${tag("mr")}

  """

  def mrStats = prop((facts: FactsWithDictionary, nan: Double) => !nan.isNaN ==> {
    val expected = FactStatistics(numericalStats(facts.facts), categoricalStats(facts.facts), FactStatisticsVersion.V1)
    (for {
      repo    <- RepositoryBuilder.repository
      _       <- RepositoryBuilder.createRepo(repo, facts.dictionary, List(facts.facts))
      factset <- Factsets.factset(repo, FactsetId.initial)
      datasets = Datasets.empty.add(Priority.Min, Dataset.factset(factset))
      outKey  <- Repository.tmpDir("stats")
      outLoc  <- repo.toIvoryLocation(outKey).asHdfsIvoryLocation
      _       <- FactStatsJob.run(repo, facts.dictionary, Datasets.empty.add(Priority.Min, Dataset.factset(factset)), outLoc)
      stats   <- FactStatisticsStorage.fromMrOutput(outLoc)
    } yield stats) must beOkLike(stats =>
      (stats.map(s => ComparisonHelpers.replaceNaN(sortStats(s), nan)) ==== ComparisonHelpers.replaceNaN(sortStats(expected), nan).right))
  }).set(minTestsOk = 10)

  def sortStats(stats: FactStatistics): FactStatistics =
    stats.copy(
      numerical = stats.numerical.sortBy(x => x.featureId -> x.date),
      categorical = stats.categorical.sortBy(x => x.featureId -> x.date))

  def numericalStats(facts: List[Fact]): List[NumericalFactStatistics] =
    facts.collect(f => f.value match {
      case IntValue(i)    => (f.featureId, f.date, 1l, i.toDouble, i.toDouble * i.toDouble)
      case LongValue(l)   => (f.featureId, f.date, 1l, l.toDouble, l.toDouble * l.toDouble)
      case DoubleValue(d) => (f.featureId, f.date, 1l, d, d * d)
      }).groupBy({ case (fid, date, _, _, _) => fid -> date }).map({ case ((fid, date), fs) =>
        val count = fs.length
        val mean = fs.map(_._4).sum / count
        val sqsum = fs.map(_._5).sum
        NumericalFactStatistics(fid, date, count, mean, Math.sqrt(sqsum / count - mean * mean))
      }).toList

  def categoricalStats(facts: List[Fact]): List[CategoricalFactStatistics] =
    facts.groupBy(f => f.featureId -> f.date).flatMap({ case ((fid, date), fs) =>
      val hist = fs.groupBy(FactStatistics.valueToCategory).map({ case (k, vs) => k -> vs.length.toLong })
      (hist.size < FactStatsCombiner.MaxCategories).option(CategoricalFactStatistics(fid, date, hist))
    }).toList
}

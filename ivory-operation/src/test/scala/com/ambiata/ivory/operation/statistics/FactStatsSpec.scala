package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._, arbitraries._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.ivory.storage.statistics._

import org.specs2._
import org.specs2.execute.{Result, AsResult}

import com.ambiata.mundane.testing.RIOMatcher._

import com.ambiata.notion.core._

import com.ambiata.poacher.hdfs.Hdfs

import scalaz.effect.IO

class FactStatsSpec extends Specification with ScalaCheck { def is = s2"""

  FactStats correctly writes factset stats out to _stats file             $factset    ${tag("mr")}

  """

  def factset = prop((facts: FactsWithDictionary, nan: Double) => !nan.isNaN ==> {
    (for {
      repo   <- RepositoryBuilder.repository
      _      <- RepositoryBuilder.createRepo(repo, facts.dictionary, List(facts.facts))
      stats1 <- FactStats.factset(repo, FactsetId.initial)
      stats2 <- FactStatisticsStorage.fromKeyStore(repo, Repository.factset(FactsetId.initial) / "_stats")
    } yield (stats1, stats2)) must beOkLike({ case (s1, s2) =>
      (s1.map(s => ComparisonHelpers.replaceNaN(s, nan)) ==== s2.map(s => ComparisonHelpers.replaceNaN(s, nan))) and (s1.isRight ==== true)
    })
  }).set(minTestsOk = 5)
}

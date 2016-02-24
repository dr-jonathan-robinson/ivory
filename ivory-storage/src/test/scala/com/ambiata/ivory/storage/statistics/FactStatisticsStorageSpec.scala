package com.ambiata.ivory.storage.statistics

import FactStatisticsStorage._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.notion.core._

import org.specs2._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

class FactStatisticsStorageSpec extends Specification with ScalaCheck { def is = s2"""

FactStatistics Storage Spec
-------------------------------

  Symmetric read / write with Key                              $symmetricKey
  Can read mr output statistics                                $mrOutput

"""
  val key: Key = Repository.root / "stats"

  def symmetricKey = propNoShrink((stats: FactStatistics) =>
    (for {
      repo <- RepositoryBuilder.repository
      _    <- FactStatisticsStorage.toKeyStore(repo, key, stats)
      s    <- FactStatisticsStorage.fromKeyStore(repo, key)
    } yield s) must beOkValue(stats.right)
  ).set(minTestsOk = 20)

  def mrOutput = prop((stats: FactStatistics) => (for {
    repo <- RepositoryBuilder.repository
    _    <- repo.store.linesUtf8.write(key / "numerical" / "part-r-00000", stats.numerical.map(_.asJson.nospaces))
    _    <- repo.store.linesUtf8.write(key / "categorical" / "part-r-00000", stats.categorical.map(_.asJson.nospaces))
    s    <- FactStatisticsStorage.fromMrOutput(repo.toIvoryLocation(key))
  } yield s) must beOkLike(s =>
    (s ==== stats.right) and ((stats.numerical.isEmpty && stats.categorical.isEmpty) ==== false)))
}

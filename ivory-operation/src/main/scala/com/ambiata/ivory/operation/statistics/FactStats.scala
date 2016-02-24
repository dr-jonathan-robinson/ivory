package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.ivory.storage.statistics._

import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.mundane.control._

import scalaz._, Scalaz._

object FactStats {

  /** Create numerical and categorical stats for a given factset.
   *  Store them as a json blob in `_stats` file under the factset dir */
  def factset(repo: Repository, factsetId: FactsetId): RIO[String \/ FactStatistics] = for {
    hdfs       <- repo.asHdfsRepository
    factset    <- Factsets.factset(repo, factsetId)
    statsKey    = Repository.factset(factsetId) / "_stats"
    output      = hdfs.toIvoryLocation(statsKey)
    exists     <- Hdfs.exists(output.toHdfsPath).run(hdfs.configuration)
    _          <- RIO.when(exists, RIO.fail(s"Factset ${factsetId.render} already has stats!"))
    dictionary <- Metadata.latestDictionaryFromIvory(repo)
    datasets    = Datasets.empty.add(Priority.Min, Dataset.factset(factset))
    tmpLoc     <- Repository.tmpLocation(hdfs, "stats").flatMap(_.asHdfsIvoryLocation)
    _          <- FactStatsJob.run(hdfs, dictionary, datasets, tmpLoc)
    stats      <- FactStatisticsStorage.fromMrOutput(tmpLoc)
    _          <- stats.traverseU(s => FactStatisticsStorage.toKeyStore(repo, statsKey, s))
  } yield stats
}

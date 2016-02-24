package com.ambiata.ivory.storage.statistics

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

object FactStatisticsStorage {

  def toKeyStore(repo: Repository, key: Key, stats: FactStatistics): RIO[Unit] =
    repo.store.utf8.write(key, toJsonString(stats))

  def fromKeyStore(repo: Repository, key: Key): RIO[String \/ FactStatistics] =
    repo.store.utf8.read(key).map(fromJsonString)

  def fromJsonString(str: String): String \/ FactStatistics =
    Parse.decodeEither[FactStatistics](str)

  def toJsonString(stats: FactStatistics): String =
    stats.asJson.spaces2

  def fromMrOutput(location: IvoryLocation): RIO[String \/ FactStatistics] = for {
    numerical   <- readJsonLines[NumericalFactStatistics](location </> DirPath.unsafe("numerical"))
    categorical <- readJsonLines[CategoricalFactStatistics](location </> DirPath.unsafe("categorical"))
  } yield for {
    _ <- (numerical.isEmpty && categorical.isEmpty).option("No stats to read").toLeftDisjunction(())
    n <- numerical.getOrElse(Nil.right)
    c <- categorical.getOrElse(Nil.right)
  } yield FactStatistics(n, c, FactStatisticsVersion.V1)

  def readJsonLines[A: CodecJson](location: IvoryLocation): RIO[Option[String \/ List[A]]] = for {
    e <- IvoryLocation.exists(location)
    r <- e.option(IvoryLocation.readLines(location).map(_.toList).map(_.traverseU(Parse.decodeEither[A]))).sequenceU
  } yield r

  implicit def FactStatisticsCodecJson: CodecJson[FactStatistics] =
    casecodec3(FactStatistics.apply, FactStatistics.unapply)("numerical", "categorical", "version")

  implicit def NumericalCodecJson: CodecJson[NumericalFactStatistics] =
    casecodec5(NumericalFactStatistics.apply, NumericalFactStatistics.unapply)("featureId", "date", "count", "sum", "sqsum")

  implicit def CategoricalCodecJson: CodecJson[CategoricalFactStatistics] =
    casecodec3(CategoricalFactStatistics.apply, CategoricalFactStatistics.unapply)("featureId", "date", "histogram")
}

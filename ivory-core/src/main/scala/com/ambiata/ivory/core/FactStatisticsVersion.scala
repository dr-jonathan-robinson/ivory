package com.ambiata.ivory.core

import argonaut._
import scalaz._


/** This represents the version of the on-disk statistics for a factset. */
sealed trait FactStatisticsVersion

object FactStatisticsVersion {
  /** V1 is json with numerical and categorical stats */
  case object V1 extends FactStatisticsVersion

  /* NOTE Don't forget the arbitrary if you add a version. */

  implicit def FactStatisticsVersionEqual: Equal[FactStatisticsVersion] =
    Equal.equalA[FactStatisticsVersion]

  implicit def FactStatisticsVersionCodecJson: CodecJson[FactStatisticsVersion] =
    ArgonautPlus.codecEnum("FactStatisticsVersion", {
      case V1 => "v1"
    }, {
      case "v1" => V1
    })
}


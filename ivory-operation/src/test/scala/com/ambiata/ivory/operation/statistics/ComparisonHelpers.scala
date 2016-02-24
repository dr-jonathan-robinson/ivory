package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core.FactStatistics

object ComparisonHelpers {
  /** Need to do this because "NaN".toDouble != "NaN".toDouble */
  def replaceNaN(stats: FactStatistics, replacement: Double): FactStatistics =
    FactStatistics(stats.numerical.map(n => if(n.sqsum.isNaN) n.copy(sqsum = replacement) else n), stats.categorical, stats.version)
}

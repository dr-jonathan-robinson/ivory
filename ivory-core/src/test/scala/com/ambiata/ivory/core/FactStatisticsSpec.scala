package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries._, Arbitraries._

import org.specs2._

class FactStatisticsSpec extends Specification with ScalaCheck { def is = s2"""

Combinators
-----------

   append works                                              $append

"""

  def append = prop((s1: FactStatistics, s2: FactStatistics) =>
    ((s1 +++ s2).numerical ==== s1.numerical ++ s2.numerical) and
    ((s1 +++ s2).categorical ==== s1.categorical ++ s2.categorical))
}

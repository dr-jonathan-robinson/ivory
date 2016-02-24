package com.ambiata.ivory.operation.extraction

import com.ambiata.mundane.testing.RIOMatcher._

import org.specs2._

class ExtractionConfigSpec extends Specification with ScalaCheck { def is = s2"""

 Can create config from system properties          $sysProps

"""

  def sysProps = prop((reducers: Option[Int]) => {
    reducers.foreach(r => System.setProperty(ExtractionConfig.GroupByEntityNumReducers, r.toString))
    val r = ExtractionConfig.fromProperties must beOkValue(ExtractionConfig(reducers))
    System.clearProperty(ExtractionConfig.GroupByEntityNumReducers)
    r
  })
}

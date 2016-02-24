package com.ambiata.ivory.core

case class FactStatistics(numerical: List[NumericalFactStatistics], categorical: List[CategoricalFactStatistics], version: FactStatisticsVersion) {
  def +++(other: FactStatistics): FactStatistics =
    FactStatistics(numerical ++ other.numerical, categorical ++ other.categorical, version)
}

object FactStatistics {
  def valueToCategory(fact: Fact): String = fact.value match {
    case IntValue(i)      => i.toString
    case LongValue(l)     => l.toString
    case DoubleValue(d)   => d.toString
    case TombstoneValue   => "☠"
    case StringValue(s)   => s
    case BooleanValue(b)  => b.toString
    case DateValue(r)     => r.hyphenated
    case ListValue(v)     => "List entries"
    case StructValue(m)   => "Struct entries"
  }
}

case class NumericalFactStatistics(featureId: FeatureId, date: Date, count: Long, mean: Double, sqsum: Double)
case class CategoricalFactStatistics(featureId: FeatureId, date: Date, histogram: Map[String, Long])

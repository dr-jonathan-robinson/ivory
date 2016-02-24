package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.scalacheck._

case class ValuesWithDate[A](xs: List[(A, Date)]) {

  lazy val ds: List[(A, Date)] =
    xs.map(td => td._1 -> td._2).sortBy(_._2)

  lazy val offsets: DateOffsets =
    DateOffsets.compact(
      ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue)
    )

  def map[B](f: A => B): ValuesWithDate[B] =
    new ValuesWithDate[B](xs.map(x => f(x._1) -> x._2))
}

object ValuesWithDate {

  implicit def ValuesWithDateArbitrary[A: Arbitrary]: Arbitrary[ValuesWithDate[A]] =
    Arbitrary(Arbitrary.arbitrary[List[(A, Date)]].map(ValuesWithDate.apply))
}

case class OptValuesWithDate[A](xs: List[(Option[A], Date)]) {

  lazy val ds: List[(Option[A], Date)] =
    xs.map(td => td._1 -> td._2).sortBy(_._2)

  lazy val offsets: DateOffsets =
    DateOffsets.compact(
      ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue)
    )

  def map[B](f: A => B): OptValuesWithDate[B] =
    new OptValuesWithDate[B](xs.map(x => (x._1).map(f) -> x._2))
}

object OptValuesWithDate {

  implicit def OptValuesWithDateArbitrary[A: Arbitrary]: Arbitrary[OptValuesWithDate[A]] =
    Arbitrary(Arbitrary.arbitrary[List[(Option[A], Date)]].map(OptValuesWithDate.apply))
}

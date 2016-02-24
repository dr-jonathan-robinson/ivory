package com.ambiata.ivory

import com.ambiata.ivory.storage._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.core.Arbitraries._

import com.ambiata.disorder._
import org.scalacheck._, Arbitrary._
import org.specs2._

class StoragePackageSpec extends Specification with ScalaCheck { def is = s2"""

Package object tests
--------------------

  Can filter hidden keys                             $filterHiddenKeys
  Can filter hidden locations                        $filterHiddenLocations

"""

  def filterHiddenKeys = prop((hidden: List[HiddenKey], nonHidden: List[NonHiddenKey]) =>
    (hidden.map(_.value) ++ nonHidden.map(_.value)).filterHidden ==== nonHidden.map(_.value))

  def filterHiddenLocations = prop((hidden: List[HiddenLocation], nonHidden: List[NonHiddenLocation]) =>
    (hidden.map(_.value) ++ nonHidden.map(_.value)).filterHidden ==== nonHidden.map(_.value))

  /**
   * WARNING The following is specific to testing the above code and is not intended for IO or to be general
   */
   
  case class HiddenKey(value: Key)
  case class NonHiddenKey(value: Key)

  case class HiddenLocation(value: Location)
  case class NonHiddenLocation(value: Location)

  implicit def KeyArbitrary: Arbitrary[Key] =
    Arbitrary(GenPlus.listOfSized(0, 20, for {
      p <- Gen.oneOf("" :: HiddenChars)
      k <- arbitrary[KeyName]
    } yield KeyName.unsafe(p + k.name)).map(_.toVector).map(Key.apply))

  implicit def HiddenKeyArbitrary: Arbitrary[HiddenKey] =
    Arbitrary(for {
      key <- arbitrary[Key]
      n   <- arbitrary[KeyName]
      p   <- Gen.oneOf(HiddenChars)
    } yield HiddenKey(key / KeyName.unsafe(p + n.name)))

  implicit def NonHiddenKeyArbitrary: Arbitrary[NonHiddenKey] =
    Arbitrary(for {
      key <- arbitrary[Key]
      n   <- arbitrary[KeyName]
    } yield NonHiddenKey(key / n))

  implicit def KeyNameArbitrary: Arbitrary[KeyName] =
    Arbitrary(for {
      chars <- GenPlus.listOfSized(1, 20, Gen.frequency(8 -> Gen.alphaNumChar, 2 -> Gen.oneOf(HiddenChars)))
      char  <- Gen.alphaNumChar
    } yield KeyName.unsafe((char :: chars).mkString))

  implicit def HiddenLocationArbitrary: Arbitrary[HiddenLocation] =
    Arbitrary(for {
      k <- arbitrary[KeyName]
      p <- Gen.oneOf(HiddenChars)
      l <- arbitrary[Location].map(_ </> FileName.unsafe(p + k.name))
    } yield HiddenLocation(l))

  implicit def NonHiddenLocationArbitrary: Arbitrary[NonHiddenLocation] =
    Arbitrary(for {
      k <- arbitrary[KeyName]
      l <- arbitrary[Location].map(_ </> FileName.unsafe(k.name))
    } yield NonHiddenLocation(l))
}

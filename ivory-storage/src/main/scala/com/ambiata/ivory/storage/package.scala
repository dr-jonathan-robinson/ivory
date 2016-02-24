package com.ambiata.ivory

import com.ambiata.mundane.io._
import com.ambiata.notion.core._

package object storage {

  val HiddenChars = List("_", ".")

  implicit class FilterHiddenKeysSyntax(keys: List[Key]) {
    def filterHidden: List[Key] =
      keys.filter(key => key.components.lastOption.map(_.name).map(filterHiddenName).getOrElse(true))
  }

  implicit class FilterHiddenLocationSyntax(locations: List[Location]) {
    def filterHidden: List[Location] =
      locations.filter(l => filterHiddenName((l match {
        case hl @ HdfsLocation(_)    => hl.filePath
        case sl @ S3Location(_, key) => FilePath.unsafe(key)
        case ll @ LocalLocation(_)   => ll.filePath
      }).basename.name))
  }

  def filterHiddenName(name: String): Boolean =
    !HiddenChars.exists(name.startsWith)

}

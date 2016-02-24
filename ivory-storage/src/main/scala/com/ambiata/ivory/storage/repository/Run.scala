package com.ambiata.ivory.storage.repository

import scalaz._, effect.IO
import com.ambiata.saws.core._
import com.ambiata.mundane.control._

/** This is a set of interfaces used to represent rank-n functions to force different computations to RIO[_]  */

trait S3Run {
  def runS3[A](action: S3Action[A]): RIO[A]
}

object S3Run {
  def apply: S3Run = new S3Run {
    def runS3[A](action: S3Action[A]): RIO[A] = action.eval

  }
}

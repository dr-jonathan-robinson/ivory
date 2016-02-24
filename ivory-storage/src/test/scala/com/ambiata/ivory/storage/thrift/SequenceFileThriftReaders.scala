package com.ambiata.ivory.storage.thrift

import com.ambiata.ivory.storage._

import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.BytesWritable

import scalaz._, Scalaz._

object SequenceFileThriftReaders {
  def readValues[A <: ThriftLike](location: Location, locationIO: LocationIO, newThrift: () => A): RIO[List[A]] = {
    val deserialiser = ThriftSerialiser()
    for {
      files <- locationIO.list(location)
      facts <- files.filterHidden.traverseU(l => SequenceFileIO.readValues(l, locationIO, new BytesWritable)(bw =>
        deserialiser.fromBytes1(newThrift, bw.copyBytes).right).flatMap(RIO.fromDisjunctionString))
    } yield facts.flatten
  }
}

package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._

import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import org.apache.hadoop.io._

import scalaz._, Scalaz._

/** Sequence file Fact readers for testing */
object SequenceFileFactReaders {

  def readPartitionFacts(location: Location, locationIO: LocationIO, partition: Partition): RIO[List[Fact]] =
    readFacts(location, locationIO, PartitionFactConverter(partition), NullWritable.get, new BytesWritable)

  def readMutableFacts(location: Location, locationIO: LocationIO): RIO[List[Fact]] =
    readFacts(location, locationIO, MutableFactConverter(), NullWritable.get, new BytesWritable)

  def readNamespaceDateFacts(location: Location, locationIO: LocationIO, namespace: Namespace): RIO[List[Fact]] =
    readFacts(location, locationIO, NamespaceDateFactConverter(namespace), new IntWritable, new BytesWritable)

  def readFacts[K <: Writable : Manifest, V <: Writable : Manifest](location: Location, locationIO: LocationIO, converter: MrFactConverter[K, V],
                                              emptyKey: => K, emptyValue: => V): RIO[List[Fact]] =
    SequenceFileIO.readKeyValues(location, locationIO, emptyKey, emptyValue)((k, v) => {
      val fact = createMutableFact
      converter.convert(fact, k, v)
      fact.right
    }).flatMap(RIO.fromDisjunctionString)
}

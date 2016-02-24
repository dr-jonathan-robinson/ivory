package com.ambiata.ivory
package storage
package repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.task.{FactsetWritable, FactsetJob}
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.partition._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{BytesQuantity, FilePath}
import com.ambiata.poacher.mr._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}

import scalaz.{Reducer => _}

/**
 * This is a hand-coded MR job to read facts from factsets as
 * Thrift records and write them out again, sorted and compressed
 */
object RecreateFactsetJob {

  /**
   * Run the MR job
   *
   * In order to reduce the amount of transferred data between the mappers and the reducers we use
   * the dictionary and create lookup tables for each fact
   *
   *
   *
   *
   */
  def run(repository: HdfsRepository, dictionary: Dictionary, namespaces: List[(Namespace, BytesQuantity)],
          factset: Factset, target: Path, reducerSize: BytesQuantity): RIO[Unit] = {
    val reducerLookups = ReducerLookups.createLookups(dictionary, namespaces, reducerSize)
    val job = Job.getInstance(repository.configuration)
    val ctx = FactsetJob.configureJob("ivory-recreate-factset", job, dictionary, reducerLookups, target, repository.codec)

    /** input */
    val partitions = Partitions.globs(repository, factset.id, factset.partitions.map(_.value))
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])
    FileInputFormat.addInputPaths(job, partitions.mkString(","))

    /** map */
    val mapperClass = factset.format match {
      case FactsetFormat.V1 => classOf[RecreateFactsetV1Mapper]
      case FactsetFormat.V2 => classOf[RecreateFactsetV2Mapper]
    }
    job.setMapperClass(mapperClass)

    /** run job */
    if (!job.waitForCompletion(true))
      Crash.error(Crash.RIO, "ivory recreate factset failed.")

    /** commit files to factset */
    Committer.commit(ctx, {
      case "factset" => target
    }, true).run(repository.configuration)
  }
}

abstract class RecreateFactsetMapper[K <: Writable] extends Mapper[K, BytesWritable, BytesWritable, BytesWritable] with MrFactFormat[K, BytesWritable] {
  type MapperContext = Mapper[K, BytesWritable, BytesWritable, BytesWritable]#Context

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  /** The output key, only create once per mapper. */
  val kout = FactsetWritable.create

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  var featureIdLookup = new FeatureIdLookup

   /** The format the mapper is reading from, set once per mapper from the subclass */
  val format: FactsetFormat

  /** Class to convert a key/value into a Fact based of the version, created once per mapper */
  var converter: MrFactConverter[K, BytesWritable] = null

  /** Empty Fact, created once per mapper and mutated for each record */
  val fact = createMutableFact

  override def setup(context: MapperContext): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, featureIdLookup)
    converter = factConverter(MrContext.getSplitPath(context.getInputSplit))
  }

  val serializer = ThriftSerialiser()

  override def map(key: K, value: BytesWritable, context: MapperContext): Unit = {
    converter.convert(fact, key, value)
    val k = featureIdLookup.ids.get(fact.featureId.toString).toInt

    FactsetWritable.set(fact, kout, k)

    val v = serializer.toBytes(fact.toThrift)
    vout.set(v, 0, v.length)

    context.write(kout, vout)
  }

}

class RecreateFactsetV1Mapper extends RecreateFactsetMapper[NullWritable] with MrFactsetFactFormatV1
class RecreateFactsetV2Mapper extends RecreateFactsetMapper[NullWritable] with MrFactsetFactFormatV2

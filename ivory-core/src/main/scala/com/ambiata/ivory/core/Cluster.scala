package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.distcopy._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.fs.Path


case class Cluster(root: Path, conf: DistCopyConfiguration, codec: Option[CompressionCodec]) {
  def hdfsConfiguration: Configuration = conf.hdfs
  def s3Client: AmazonS3Client = conf.client
  def rootDirPath: DirPath = DirPath.unsafe(root.toString)

  def io: LocationIO =
    LocationIO(hdfsConfiguration, s3Client)

  /** A very short term convenience method - we need to remove this soon */
  def toIvoryLocation(l: Location): IvoryLocation =
    IvoryLocation.fromLocation(l, Cluster.ivoryConfiguration(this))
}

object Cluster {
  def fromIvoryConfiguration(root: Path, ivory: IvoryConfiguration, mappers: Int): Cluster = {
    val conf = DistCopyConfiguration(
        ivory.configuration
      , ivory.s3Client
      , DistCopyParameters(
          mappers
        , DistCopyMapperParameters(
            DistCopyMapperParameters.Default.retryCount
          , DistCopyMapperParameters.Default.partSize
          , DistCopyMapperParameters.Default.readLimit
          , DistCopyMapperParameters.Default.multipartUploadThreshold)))
    Cluster(root, conf, ivory.codec)
  }

  def ivoryConfiguration(cluster: Cluster): IvoryConfiguration =
    new IvoryConfiguration(
        cluster.s3Client
      , () => cluster.hdfsConfiguration
      , () => cluster.codec
    )

}

package com.ambiata.ivory.core

import java.io.File
import java.net.URI

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._
import org.specs2._, matcher._, execute.{Result => SpecsResult}
import org.specs2.matcher.DisjunctionMatchers

class RepositorySpec extends Specification with ScalaCheck with DisjunctionMatchers { def is = s2"""

Repository Known Answer Tests
-----------------------------

  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3
  Can parse local URIs                            $local
  Can parse relative URIs                         $relative
  Can parse default local URIs                    $dfault
  Can parse default relative local URIs           $fragment

"""

  def hdfs =
    withConf(conf => Repository.parseUri("hdfs:///some/path", conf) must
      be_\/-(HdfsRepository(HdfsLocation("/some/path"), conf)))

  def s3 =
    withConf(conf => Repository.parseUri("s3://bucket/key", conf) must
      be_\/-(S3Repository(S3Location("bucket", "key"), conf)))

  def local =
    withConf(conf => Repository.parseUri("file:///some/path", conf) must
      be_\/-(LocalRepository(LocalLocation("/some/path"))))

  def relative =
    withConf(conf => Repository.parseUri("file:some/path", conf) must
      be_\/-(LocalRepository(LocalLocation("some/path"))))

  def dfault =
    withConf(conf => Repository.parseUri("/some/path", conf) must
      be_\/-(HdfsRepository(HdfsLocation("/some/path"), conf)))

  def fragment =
    withConf(conf => Repository.parseUri("some/path", conf) must
      be_\/-(HdfsRepository(HdfsIvoryLocation(HdfsLocation((DirPath.unsafe(new File(".").getAbsolutePath).dirname </> "some" </> "path").path), conf.configuration, conf.codec))))

  def withConf(f: IvoryConfiguration => SpecsResult): MatchResult[RIO[IvoryConfiguration]] =
    IvoryConfigurationTemporary.random.conf must beOkLike(f)
}

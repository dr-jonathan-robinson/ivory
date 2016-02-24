package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.poacher.mr.ThriftSerialiser
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.saws.core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.BytesWritable

import scalaz.{DList => _, _}, Scalaz._

object RepositoryBuilder {
  def repository: RIO[HdfsRepository] = for {
    d <- LocalTemporary.random.directory
    _ <- Directories.mkdirs(d)
    c <- IvoryConfigurationTemporary(d.path).conf
    r = HdfsRepository(HdfsLocation(d.path), c)
  } yield r

  def using[A](f: HdfsRepository => RIO[A]): RIO[A] =
    repository >>= (f(_))

  def createCommit(repo: HdfsRepository, dictionary: Dictionary, facts: List[List[Fact]]): RIO[Commit] = for {
    d          <- createDictionary(repo, dictionary)
    r          <- createFactsets(repo, facts)
    (s, _)     =  r
    c          <- CommitStorage.findOrCreateLatestId(repo, d, s)
    store      <- FeatureStoreTextStorage.fromId(repo, s)
  } yield Commit(c, Identified(d, dictionary), store, None)

  def createDictionary(repo: HdfsRepository, dictionary: Dictionary): RIO[DictionaryId] =
    DictionaryThriftStorage(repo).store(dictionary)

  def createRepo(repo: HdfsRepository, dictionary: Dictionary, facts: List[List[Fact]]): RIO[FeatureStoreId] = for {
    _      <- createDictionary(repo, dictionary)
    stores <- createFactsets(repo, facts)
  } yield stores._1

  def createFactset(repo: HdfsRepository, facts: List[Fact]): RIO[FactsetId] =
    createFactsets(repo, List(facts)).map(_._2.head)

  def createFactsets(repo: HdfsRepository, facts: List[List[Fact]]): RIO[(FeatureStoreId, List[FactsetId])] = {
    facts.foldLeftM(NonEmptyList(FactsetId.initial))({ case (factsetIds, facts) =>
      val groups = facts.groupBy(f => Partition(f.namespace, f.date)).toList
      groups.traverse({
        case (partition, facts) =>
          val out = repo.toIvoryLocation(Repository.factset(factsetIds.head) / partition.key / "data-output").location
          writeThriftValues(repo.configuration, facts.map(_.toThrift), out)
      }).as(factsetIds.head.next.get <:: factsetIds)
    }).map(_.tail.reverse).flatMap(factsets =>
      RepositoryT.runWithRepo(repo, writeFactsetVersion(factsets)).map(_.last -> factsets))
  }

  def writeThriftValues[A <: ThriftLike](config: Configuration, thrift: List[A], out: Location): RIO[Unit] =
    SequenceFileIO.writeValues(out, LocationIO(config, Clients.s3), thrift)(t => new BytesWritable(ThriftSerialiser().toBytes(t)))

  def uniqueFacts(allFacts: List[Fact]): List[Fact] =
    allFacts.filter(!_.isTombstone).groupBy(f => (f.entity, f.featureId)).toList.map(_._2.maxBy(_.date.int))

  def createSquash(repo: HdfsRepository, allFacts: List[Fact]): RIO[IvoryLocation] = {
    val serialiser = ThriftSerialiser()
    for {
      out <- Repository.tmpDir("squash").map(repo.toIvoryLocation)
      _   <- SequenceFileIO.writeValues(out.location, LocationIO(repo.configuration, Clients.s3), uniqueFacts(allFacts))(f =>
        new BytesWritable(serialiser.toBytes(f.toNamespacedThrift)))
    } yield out
  }

  def readFactset(repo: HdfsRepository, factsetId: FactsetId): RIO[List[Fact]] = for {
    factset <- Factsets.factset(repo, factsetId)
    facts   <- factset.format match {
      case FactsetFormat.V1 => factset.partitions.traverseU(s => readFactsetPartition(repo, factsetId, s.value))
      case FactsetFormat.V2 => factset.partitions.traverseU(s => readFactsetPartition(repo, factsetId, s.value))
    }
  } yield facts.flatten

  def readFactsetPartition(repo: HdfsRepository, factsetId: FactsetId, partition: Partition): RIO[List[Fact]] = {
    val locationIO = LocationIO(repo.configuration, Clients.s3)
    for {
      files <- IvoryLocation.list(repo.toIvoryLocation(Repository.factset(factsetId) / partition.key))
      facts <- files.traverseU(location => SequenceFileFactReaders.readPartitionFacts(location.location, locationIO, partition))
    } yield facts.flatten
  }

  def readSnapshot(repo: HdfsRepository, snapshotId: SnapshotId): RIO[List[Fact]] = for {
    snapshot <- SnapshotStorage.byIdOrFail(repo, snapshotId)
    facts    <- snapshot.format match {
      case SnapshotFormat.V1 =>
        readMutableFacts(repo.configuration, repo.toIvoryLocation(Repository.snapshot(snapshotId)).location)
      case SnapshotFormat.V2 => for {
        nss   <- repo.store.listHeads(Repository.snapshot(snapshotId)).map(_.filterHidden)
        facts <- nss.traverseU(ns => readSnapshotNamespace(repo, snapshotId, Namespace.unsafe(ns.name)))
      } yield facts.flatten
    }
  } yield facts

  def readMutableFacts(conf: Configuration, location: Location): RIO[List[Fact]] = {
    val locationIO = LocationIO(conf, Clients.s3)
    for {
      files <- locationIO.list(location)
      facts <- files.filterHidden.traverseU(location => SequenceFileFactReaders.readMutableFacts(location, locationIO))
    } yield facts.flatten
  }

  def readSnapshotNamespace(repo: HdfsRepository, snapshotId: SnapshotId, namespace: Namespace): RIO[List[Fact]] = {
    val locationIO = LocationIO(repo.configuration, Clients.s3)
    for {
      files <- IvoryLocation.list(repo.toIvoryLocation(Repository.snapshot(snapshotId) / namespace.asKeyName))
      facts <- files.traverseU(location => SequenceFileFactReaders.readNamespaceDateFacts(location.location, locationIO, namespace))
    } yield facts.flatten
  }

  def writeFactsetVersion(factsets: List[FactsetId]): RepositoryTIO[List[FeatureStoreId]] =
    factsets.traverseU(Factsets.updateFeatureStore).map(_.flatten)


}

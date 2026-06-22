package de.otto.anthology.statestore

import de.otto.anthology.statestore.StateStore
import org.rocksdb.*
import org.rocksdb.util.SizeUnit
import ox.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** StateStore implementation, backed by [[https://github.com/facebook/rocksdb RocksDB]].
  */
class RocksDBStateStore(config: RocksDBConfig, path: String) extends StateStore:

    /** As it is known that executing native code can still lead to thread pinning, we are, to be on the safe side,
      * offloading it to a separate thread pool for the time being.
      *
      * Source: [[https://rockthejvm.com/articles/the-ultimate-guide-to-java-virtual-threads#pinned-virtual-threads]]
      */
    private def runBlocking[T](body: => T): T =
        Future(body)(using RocksDBStateStore.threadPool).get()

    private val db: RocksDB =
        RocksDB.loadLibrary()
        val opts = configure()
        Files.createDirectories(Paths.get(path))
        runBlocking:
            RocksDB.open(opts, path)

    private def configure(): Options =

        val options: Options = new Options()

        val tableOptions: BlockBasedTableConfig =
            Option(options.tableFormatConfig())
                .collect:
                    case bbtc: BlockBasedTableConfig => bbtc
                .getOrElse(new BlockBasedTableConfig())

        // Setup basic options
        options
            .setCreateIfMissing(true) // If the database doesn't exist, create it.
            .setErrorIfExists(false) // If the database already exists, don't raise an error.
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setCompactionStyle(CompactionStyle.UNIVERSAL)

        // Setup cache
        val cacheSize = config.cacheSizeMb * SizeUnit.MB
        val cache = LRUCache(cacheSize)

        // Setup block cache
        tableOptions.setBlockCache(cache)
        tableOptions.setCacheIndexAndFilterBlocks(true)
        tableOptions.setCacheIndexAndFilterBlocksWithHighPriority(true)
        tableOptions.setPinTopLevelIndexAndFilter(true)

        // Setup write buffer
        val writeBufferSize = config.writeBufferSizeMb * SizeUnit.MB
        val writeBufferManager = new WriteBufferManager(writeBufferSize, cache)
        options.setWriteBufferManager(writeBufferManager)

        // Setup bloom filter
        val bloomFilter = BloomFilter()
        tableOptions.setFilterPolicy(bloomFilter)
        tableOptions.setOptimizeFiltersForMemory(true)

        // Setup format version
        tableOptions.setFormatVersion(7)

        options.setTableFormatConfig(tableOptions)

        options

    override def get(key: String): Option[Array[Byte]] =
        runBlocking:
            Option(db.get(key.getBytes(StandardCharsets.UTF_8)))

    override def put(key: String, value: Array[Byte]): Unit =
        runBlocking:
            db.put(key.getBytes(StandardCharsets.UTF_8), value)

    override def delete(key: String): Unit =
        runBlocking:
            db.delete(key.getBytes(StandardCharsets.UTF_8))

    def close(): Unit =
        runBlocking:
            db.close()

object RocksDBStateStore:
    val threadPool: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

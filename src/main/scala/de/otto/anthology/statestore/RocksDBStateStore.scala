package de.otto.anthology.statestore

import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStore.BatchOperation
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.BloomFilter
import org.rocksdb.CompressionType
import org.rocksdb.LRUCache
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteBufferManager
import org.rocksdb.WriteOptions
import org.rocksdb.util.SizeUnit
import ox.computeIntensive

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/** StateStore implementation, backed by [[https://github.com/facebook/rocksdb RocksDB]].
  */
class RocksDBStateStore(config: RocksDBConfig, path: String) extends StateStore:

    /** As it is known that executing native code can still lead to thread pinning, we are offloading it to a separate
      * thread pool.
      *
      * Sources: [[https://rockthejvm.com/articles/the-ultimate-guide-to-java-virtual-threads#pinned-virtual-threads]],
      * [[https://stevenpg.com/posts/virtual-thread-pinning-2026-jep-491/]]
      */
    private def runBlocking[T](body: => T): T =
        computeIntensive(RocksDBStateStore.threadPool)(body)

    private val db: RocksDB =
        RocksDB.loadLibrary()
        val opts = configure()
        Files.createDirectories(Paths.get(path))
        runBlocking:
            RocksDB.open(opts, path)

    private def configure(): Options =

        val options: Options = new Options()
        options.close()

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
            .setBestEffortsRecovery(config.bestEffortsRecovery)

        if config.cacheSizeMb > 0L then
            // Setup cache
            val cacheSize = config.cacheSizeMb * SizeUnit.MB
            val cache = LRUCache(cacheSize)

            // Setup block cache
            tableOptions.setNoBlockCache(false)
            tableOptions.setBlockCache(cache)
            tableOptions.setCacheIndexAndFilterBlocks(true)
            tableOptions.setCacheIndexAndFilterBlocksWithHighPriority(true)
            tableOptions.setPinTopLevelIndexAndFilter(true)

            // Setup write buffer
            val writeBufferSize = config.writeBufferSizeMb * SizeUnit.MB
            val writeBufferManager = new WriteBufferManager(writeBufferSize, cache)
            options.setWriteBufferManager(writeBufferManager)
        else
            tableOptions.setNoBlockCache(true)
            tableOptions.setCacheIndexAndFilterBlocks(false)
            tableOptions.setCacheIndexAndFilterBlocksWithHighPriority(false)
            tableOptions.setPinTopLevelIndexAndFilter(false)

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

    override def writeBatch(ops: Seq[BatchOperation]): Unit =
        if ops.nonEmpty then
            runBlocking:
                val batch = WriteBatch()
                try
                    ops.foreach:
                        case BatchOperation.Put(key, value) =>
                            batch.put(key.getBytes(StandardCharsets.UTF_8), value)
                        case BatchOperation.Delete(key) =>
                            batch.delete(key.getBytes(StandardCharsets.UTF_8))
                    val writeOptions = WriteOptions()
                    // TODO: which options should be configured
                    try
                        db.write(writeOptions, batch)
                    finally
                        writeOptions.close()
                finally batch.close()

    def close(): Unit =
        runBlocking:
            db.close()

object RocksDBStateStore:

    /** To execute the native blocking code, we follow the thread model from cats-effect and run it on an unbounded,
      * cached pool.
      *
      * @see
      *   [[https://github.com/typelevel/cats-effect/blob/series/3.x/docs/thread-model.md#thread-blocking]]
      */
    private[RocksDBStateStore] val threadPool: ExecutorService = Executors.newCachedThreadPool()

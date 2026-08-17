package de.otto.anthology.statestore

import com.fasterxml.jackson.databind.ObjectMapper
import de.otto.anthology.MessageId
import de.otto.anthology.statestore.RocksDBConfig
import de.otto.anthology.statestore.RocksDBStateStore
import org.scalatest.BeforeAndAfterEach
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import scala.compiletime.uninitialized

class RocksDBStateStoreTest extends AnyFlatSpec, Matchers, Diagrams, BeforeAndAfterEach:
    private val mapper = new ObjectMapper()
    private var stateStore: RocksDBStateStore = uninitialized
    private var tmpFile: File = uninitialized

    def deleteDirectoryRecursively(path: Path): Unit =
        Files.walkFileTree(
            path,
            new SimpleFileVisitor[Path] {
                override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                    Files.delete(file)
                    FileVisitResult.CONTINUE
                }

                override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult =
                    Files.delete(dir)
                    FileVisitResult.CONTINUE
            }
        )

    override def beforeEach(): Unit =
        tmpFile = File.createTempFile("rocksdb_test", ".db")
        tmpFile.delete
        val config = RocksDBConfig()
        stateStore = RocksDBStateStore(config, tmpFile.getAbsolutePath)

    override def afterEach(): Unit =
        stateStore.shutdown()
        println(s"removing temporary RocksDB file: ${tmpFile.toPath}")
        deleteDirectoryRecursively(tmpFile.toPath)

    "RocksDBStateStore" should "perform get and put" in:
        val aggId = MessageId("id-123")
        val jsonStr = """{"key":"value"}"""

        stateStore.getJson(aggId.toString) shouldBe None

        stateStore.putJson(aggId.toString, mapper.readTree(jsonStr))
        val retrieved = stateStore.getJson(aggId.toString)
        retrieved shouldBe defined
        assert(retrieved.get.toString == jsonStr)

        val jsonStrUpdated = """{"key":"value-updated","new-key":"new-value"}"""
        stateStore.putJson(aggId.toString, mapper.readTree(jsonStrUpdated))
        val retrievedUpdated = stateStore.getJson(aggId.toString)
        retrievedUpdated shouldBe defined
        assert(retrievedUpdated.get.toString == jsonStrUpdated)

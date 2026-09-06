package de.otto.capella

import de.otto.capella.BroadcastStage.*
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*
import ox.channels.Channel
import ox.flow.Flow

class BroadcastStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "BroadcastStage" should "send values from one source to multiple sinks" in:
        // given
        val srcValues = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
        val src: Flow[Int] = Flow.fromIterable(srcValues)

        // when
        val targetValues =
            supervised:
                val sink1 = Channel.rendezvous[Int]
                val result1: Fork[List[Int]] = fork(Flow.fromSource(sink1).runToList())
                val sink2 = Channel.rendezvous[Int]
                val result2: Fork[List[Int]] = fork(Flow.fromSource(sink2).runToList())
                val sink3 = Channel.rendezvous[Int]
                val result3: Fork[List[Int]] = fork(Flow.fromSource(sink3).runToList())

                val resultF: List[Int] = src.broadcast(Seq(sink1, sink2, sink3)).runToList()

                (result1.join(), result2.join(), result3.join(), resultF)

        // then
        assert(targetValues._1 == srcValues)
        assert(targetValues._2 == srcValues)
        assert(targetValues._3 == srcValues)
        assert(targetValues._4 == srcValues)

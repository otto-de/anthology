package de.otto.anthology

import de.otto.anthology.MergeStage.*
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.flow.Flow

class MergeStageTest extends AnyFlatSpec, Matchers, Diagrams:

    "MergeStage" should "merge values from multiple sources" in:
        // given
        val src1Values = List(1, 2, 3)
        val src1: Flow[Int] = Flow.fromIterable(src1Values)
        val src2Values = List(4, 5, 6)
        val src2: Flow[Int] = Flow.fromIterable(src2Values)
        val src3Values = List(7, 8, 9)
        val src3: Flow[Int] = Flow.fromIterable(src3Values)
        val sources: List[Flow[Int]] = List(src1, src2, src3)

        // when
        val target: Flow[Int] = sources.merge()
        val targetValues = target.runToList()

        // then
        assert((src1Values ++ src2Values ++ src3Values) == targetValues.sorted)

    it should "mergeFair values from multiple sources" in:
        // given
        val src1Values = List(1, 2, 3)
        val src1: Flow[Int] = Flow.fromIterable(src1Values)
        val src2Values = List(4, 5, 6)
        val src2: Flow[Int] = Flow.fromIterable(src2Values)
        val src3Values = List(7, 8, 9)
        val src3: Flow[Int] = Flow.fromIterable(src3Values)
        val sources: List[Flow[Int]] = List(src1, src2, src3)

        // when
        val target: Flow[Int] = sources.mergeFair()
        val targetValues = target.runToList()

        // then
        assert((src1Values ++ src2Values ++ src3Values) == targetValues.sorted)

    it should "merge values from multiple sources fairly distributed" in:
        // given
        val src1Values = (1 to 2000)
        val src1: Flow[Int] = Flow.fromIterable(src1Values)
        val src2Values = (2001 to 4000)
        val src2: Flow[Int] = Flow.fromIterable(src2Values)
        val src3Values = (4001 to 6000)
        val src3: Flow[Int] = Flow.fromIterable(src3Values)
        val sources: List[Flow[Int]] = List(src1, src2, src3)

        // when
        val target: Flow[Int] = sources.mergeFair()
        val targetValues = target.runToList()

        // then
        // measuring fairness somehow by expecting that the values of the first half are nearly equally distributed
        // across all three sources by calculating the expected average
        val firstHalf = targetValues.take(3000)
        val firstHalfAverage = firstHalf.sum / 3000
        val tolerance = 100
        assert(firstHalfAverage >= 2500 - tolerance)
        assert(firstHalfAverage <= 2500 + tolerance)

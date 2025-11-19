package worker

import java.nio.file.{Paths, Files}

import scala.concurrent.Await

import org.scalatest.funsuite.AnyFunSuite

import common.{Data, Key}
import common.{GenData, CheckSorted}
import worker.DataProcessor.{sortAndPartitioning, removeTempDir}

class PartitioningSuite extends AnyFunSuite with GenData with CheckSorted {
  val workerInfos = List("workerC", "workerE", "workerA", "workerD", "workerB")
  val partitionRanges = List(
    (Key(Vector.fill(10)(20.toByte)), Key(Vector.fill(10)(70.toByte))),
    (Key(Vector.fill(10)(0.toByte)), Key(Vector.fill(10)(20.toByte))),
    (Key(Vector.fill(10)(150.toByte)), Key(Vector.fill(10)(200.toByte))),
    (Key(Vector.fill(10)(200.toByte)), Key(Vector.fill(10)(255.toByte))),
    (Key(Vector.fill(10)(70.toByte)), Key(Vector.fill(10)(150.toByte))))

  // workerA -> [0, 20)
  // workerB -> [20, 70)
  // workerC -> [70, 150)
  // workerD -> [150, 200)
  // workerE -> [200, 255]

  test("Test makePartition function") {

    val partitionFunc = worker.Util.makePartition(partitionRanges, workerInfos)

    val testKeys = for {
      i <- List(25, 75, 125, 175, 225, 255, 0, 50, 100, 150, 200)
    } yield Key(Vector.fill(10)(i.toByte))

    val expectedAssignments = List(
      "workerB", "workerC", "workerC", "workerD", "workerE", "workerE", 
      "workerA", "workerB", "workerC", "workerD", "workerE")

    val actualAssignments = testKeys.map(partitionFunc)

    assert(actualAssignments === expectedAssignments)
  }

  test("Test sort and partitioning function") {
    val inputDirs = List("src/test/resources/partition1-partitioning", "src/test/resources/partition2-partitioning", "src/test/resources/partition3-partitioning")

    val partitionSize = 100000

    for ((inputDir, index) <- inputDirs.zip(0 until inputDirs.length))
      generateData(inputDir, index * 100000, 100000, skewed = true)

    val partition = worker.Util.makePartition(partitionRanges, workerInfos)


    val resultDirs = Await.result(
      sortAndPartitioning(inputDirs, partition),
      scala.concurrent.duration.Duration.Inf)

    // Check whether each partitioned file is sorted
    resultDirs.foreach { dir => assert { isSorted(dir) } }

    cleanupGeneratedData()
    removeTempDir()
  }

}
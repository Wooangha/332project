package worker

import java.nio.file.{Paths, Files}

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Sequential

import common.{GenData, CheckSorted}
import worker.DataProcessor.merge
import common.Data
import worker.DataProcessor.removeTempDir

class MergeSuite extends AnyFunSuite with GenData with CheckSorted {

  val inputDirs = List("src/test/resources/partition1-merge", "src/test/resources/partition2-merge", "src/test/resources/partition3-merge")
  val sortedInputDirs = List("src/test/resources/sorted_partition1", "src/test/resources/sorted_partition2", "src/test/resources/sorted_partition3")

  test("Merge sorted partitioned files — the output file should be sorted.") {
    val partitionSize = 100000

    for ((inputDir, index) <- inputDirs.zip(0 until inputDirs.length))
      generateData(inputDir, index * partitionSize, partitionSize, skewed = false)

    def outputDirBuilder(): String = "src/test/resources/output/merged_out"

    val data = for ((inputDir, sortedInputDir) <- inputDirs.zip(sortedInputDirs)) 
      yield {
        val sortedData = Data.fromFile(inputDir).sort()
        sortedData.save(sortedInputDir)
        sortedData
      }    

    val sortedData = {
      var sortedArray = Array.empty[Datum]
      for (d <- data) {
        sortedArray ++= d.data
      }
      new Data(sortedArray).sort()
    }

    merge(sortedInputDirs, outputDirBuilder, partitionSize * inputDirs.length)

    assert { isSorted(outputDirBuilder()) }

    assert {
      val outputData = Data.fromFile(outputDirBuilder())
      outputData === sortedData
    }

    cleanupGeneratedData()

    for (sortedInputDir <- sortedInputDirs) {
      Files.deleteIfExists(Paths.get(sortedInputDir))
    }

    Files.deleteIfExists(Paths.get(outputDirBuilder()))
  }

  test("Merge sorted partitioned files — when maxSize is smaller than total data size") {
    val partitionSize = 100000

    for ((inputDir, index) <- inputDirs.zip(0 until inputDirs.length))
      generateData(inputDir, index * partitionSize, partitionSize, skewed = false)

    var outputDirs = Set[String]()

    def outputDirBuilder(): String = {
      val dir = s"src/test/resources/output/merged_out_${outputDirs.size}"
      outputDirs += dir
      dir
    }

    val data = for ((inputDir, sortedInputDir) <- inputDirs.zip(sortedInputDirs)) 
      yield {
        val sortedData = Data.fromFile(inputDir).sort()
        sortedData.save(sortedInputDir)
        sortedData
      }    

    val sortedData = {
      var sortedArray = Array.empty[Datum]
      for (d <- data) {
        sortedArray ++= d.data
      }
      new Data(sortedArray).sort()
    }

    merge(sortedInputDirs, outputDirBuilder, partitionSize)

    val outputData = outputDirs.foldLeft(new Data(Array.empty)) { (acc, dir) => 
      val outputData = Data.fromFile(dir)
      acc ++ outputData
    }

    assert { outputData === sortedData }

    for (dir <- outputDirs) {
      assert { isSorted(dir) }
      Files.deleteIfExists(Paths.get(dir))
    }

    cleanupGeneratedData()

    for (sortedInputDir <- sortedInputDirs) {
      Files.deleteIfExists(Paths.get(sortedInputDir))
    }

    Files.deleteIfExists(Paths.get(outputDirBuilder()))

  }

  test("Merge sorted partitioned files — files are too Many to fit in memory (generate 32MiB 100 files)") {
    val partitionSize = 320000

    val generateManyData = (0 until 100).map { i => 
      Future {
        val dir = s"src/test/resources/merge_many_partition_$i"
        generateData(dir, i * partitionSize, partitionSize, skewed = false)
        dir
      }
    }.toList

    val inputManyDirs = Await.result(
      Future.sequence(generateManyData),
      scala.concurrent.duration.Duration.Inf)

    val sortedInputManyDirsFutures = inputManyDirs.map { dir => 
      Future {
        val sortedDir = dir + "_sorted"
        val sortedData = Data.fromFile(dir).sort()
        sortedData.save(sortedDir)
        sortedDir
      }
    }

    val sortedInputManyDirs = Await.result(
      Future.sequence(sortedInputManyDirsFutures),
      scala.concurrent.duration.Duration.Inf)

    var outputDirs = Set[String]()
    def outputDirBuilder(): String = {
      val dir = s"src/test/resources/output/merged_out_${outputDirs.size}"
      outputDirs += dir
      dir
    }

    merge(sortedInputManyDirs, outputDirBuilder, partitionSize * 10)

    outputDirs.foreach { dir => 
      assert { isSorted(dir) }
      Files.deleteIfExists(Paths.get(dir))
    }
    sortedInputManyDirs.foreach { dir => 
      Files.deleteIfExists(Paths.get(dir))
    }

    removeTempDir()
    cleanupGeneratedData()
  }

}
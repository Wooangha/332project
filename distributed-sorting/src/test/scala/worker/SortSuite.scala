package worker

import java.nio.file.{Paths, Files}

import scala.sys.process._

import org.scalatest.funsuite.AnyFunSuite

import common.Data

class SortSuite extends AnyFunSuite with GenData with CheckSorted {

  test("Sort data read from a file and write it back to another file — the output file should be sorted.") {
    val inputDir = "src/test/resources/data"
    val outputDir = "src/test/resources/output/sorted_out"

    generateData(inputDir, 10, 1000, skewed = false)

    val data = Data.fromFile(inputDir)

    val sortedData = data.sort()

    sortedData.save(outputDir)

    assert { isSorted(outputDir) }

    cleanupGeneratedData()
    Files.deleteIfExists(Paths.get(outputDir))
  }
}

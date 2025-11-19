package worker.io

import java.nio.file.{Paths, Files}

import org.scalatest.funsuite.AnyFunSuite

import common.Data
import common.{GenData, CheckSorted}

class IOSuite extends AnyFunSuite with GenData {

  test("First, read Binary and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/bin_data"
    val outputDir = "src/test/resources/output/bin_out"
    
    generateData(inputDir, 10, 1000, skewed = false)

    val data = Data.fromFile(inputDir)

    data.save(outputDir)

    assert {
      val newData = Data.fromFile(outputDir)
      newData.data === data.data
    }
    cleanupGeneratedData()
    Files.deleteIfExists(Paths.get(outputDir))
  }

  test("DatumFileIterator should read all data correctly from a file.") {
    val inputDir = "src/test/resources/bin_data"
    generateData(inputDir, 10, 1000, skewed = false)


    val readerIterator = new DatumFileIterator(inputDir)
    val data = Data.fromFile(inputDir)

    var iterCount = 0

    data.data.zip(readerIterator).foreach { case (dataFromReader, dataFromIterator) =>
      iterCount += 1
      assert {
        dataFromIterator.key === dataFromReader.key && dataFromIterator.value === dataFromReader.value
      }
    }
    assert(iterCount === data.data.length)

    cleanupGeneratedData()
  }
}

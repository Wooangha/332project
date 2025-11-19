package worker

import org.scalatest.funsuite.AnyFunSuite

import common.Data

class SortSuite extends AnyFunSuite {

  test("Sort data read from a file and write it back to another file — the output file should be sorted.") {
    val inputDir = "src/test/resources/ascii_data"
    val inputSortedDir = "src/test/resources/sorted_ascii_data"
    val outputDir = "src/test/resources/output/sorted_out"

    val data = Data.fromFile(inputDir)

    val sortedData = data.sort()

    sortedData.save(outputDir)
    val newData = Data.fromFile(outputDir)

    val answerData = Data.fromFile(inputSortedDir)
    answerData.data.zip(newData.data).foreach { case (expected, actual) => 
      assert(expected.key === actual.key && expected.value === actual.value)
    }

    assert {
      newData.data.length === (answerData.data.length)
    }
  }
}

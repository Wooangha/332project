package worker.io

import java.nio.file.{Paths, Files}

import org.scalatest.funsuite.AnyFunSuite

class IOSuite extends AnyFunSuite {
  
  test("First, read Ascii and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/ascii_data"
    val outputDir = "src/test/resources/output/ascii_out"
    
    val data = Data.fromFile(inputDir)

    data.save(outputDir)

    assert {
      val newData = Data.fromFile(outputDir)
      newData === data
    }
  }

  test("First, read Binary and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/bin_data"
    val outputDir = "src/test/resources/output/bin_out"
    
    val data = Data.fromFile(inputDir)

    data.save(outputDir)

    assert {
      val newData = Data.fromFile(outputDir)
      newData === data
    }
  }
}

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

class ReaderIteratorSuite extends AnyFunSuite {

  test("DatumFileIterator should read all data correctly from a file.") {
    val inputDir = "src/test/resources/bin_data"
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
  }
}

package worker.io

import java.nio.file.{Paths, Files}

import org.scalatest.funsuite.AnyFunSuite

class IOSuite extends AnyFunSuite {
  
  test("First, read Ascii and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/ascii_data"
    val outputDir = "src/test/resources/output/ascii_out"
    
    val firstReader = new DatumFileReader(inputDir)
    val data = firstReader.data

    val writer = new DatumFileWriter(outputDir, data)
    writer.write()

    assert {
      val newReader = new DatumFileReader(outputDir)
      newReader.data === data
    }
  }

  test("First, read Binary and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/bin_data"
    val outputDir = "src/test/resources/output/bin_out"
    
    val firstReader = new DatumFileReader(inputDir)
    val data = firstReader.data

    val writer = new DatumFileWriter(outputDir, data)
    writer.write()

    assert {
      val newReader = new DatumFileReader(outputDir)
      newReader.data === data
    }
  }
}

class SortSuite extends AnyFunSuite {

  test("Sort data read from a file and write it back to another file — the output file should be sorted.") {
    val inputDir = "src/test/resources/ascii_data"
    val inputSortedDir = "src/test/resources/sorted_ascii_data"
    val outputDir = "src/test/resources/output/sorted_out"

    val reader = new DatumFileReader(inputDir)
    val data = reader.data

    val sortedData = DataProcessor.sort(data)

    new DatumFileWriter(outputDir, sortedData).write()
    val newReader = new DatumFileReader(outputDir)
    val newData = newReader.data
    new DatumFileReader(inputSortedDir).data.zip(newData).foreach { case (expected, actual) => 
      assert(expected.key === actual.key && expected.value === actual.value)
    }

    assert {
      newData.length === (new DatumFileReader(inputSortedDir).data.length)
    }
  }
}

class ReaderIteratorSuite extends AnyFunSuite {

  test("DatumFileIterator should read all data correctly from a file.") {
    val inputDir = "src/test/resources/bin_data"
    val readerIterator = new DatumFileIterator(inputDir)
    val reader = new DatumFileReader(inputDir)

    var iterCount = 0

    reader.data.zip(readerIterator).foreach { case (dataFromReader, dataFromIterator) =>
      iterCount += 1
      assert {
        dataFromIterator.key === dataFromReader.key && dataFromIterator.value === dataFromReader.value
      }
    }
    assert(iterCount === reader.data.length)
  }
}

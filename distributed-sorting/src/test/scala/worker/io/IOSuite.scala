package worker.io

import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.{Paths, Files}

class IOSuite extends AnyFunSuite {
  class DefaultReader(val inputDir: String) extends DataReader
  class DefaultWriter(val outputDir: String, val data: List[Datum]) extends DataWriter {
    override def write(): Unit = {
      val path = Paths.get(outputDir)
      Files.createDirectories(path.getParent)
      super[DataWriter].write()
    }
  }
  
  test("First, read Ascii and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/ascii_data"
    val outputDir = "src/test/resources/output/ascii_out"
    
    val firstReader = new DefaultReader(inputDir)
    val data = firstReader.data

    val writer = new DefaultWriter(outputDir, data)
    writer.write()

    assert {
      val newReader = new DefaultReader(outputDir)
      newReader.data === data
    }
  }

  test("First, read Binary and then write it back to a file — the two should be identical.") {
    val inputDir = "src/test/resources/bin_data"
    val outputDir = "src/test/resources/output/bin_out"
    
    val firstReader = new DefaultReader(inputDir)
    val data = firstReader.data

    val writer = new DefaultWriter(outputDir, data)
    writer.write()

    assert {
      val newReader = new DefaultReader(outputDir)
      newReader.data === data
    }
  }
}

class SortSuite extends AnyFunSuite {
  class DefaultReader(val inputDir: String) extends DataReader
  class DefaultWriter(val outputDir: String, val data: List[Datum]) extends DataWriter {
    override def write(): Unit = {
      val path = Paths.get(outputDir)
      Files.createDirectories(path.getParent)
      super[DataWriter].write()
    }
  }

  test("Sort data read from a file and write it back to another file — the output file should be sorted.") {
    val inputDir = "src/test/resources/ascii_data"
    val inputSortedDir = "src/test/resources/sorted_ascii_data"
    val outputDir = "src/test/resources/output/sorted_out"

    val reader = new DefaultReader(inputDir)
    val data = reader.data

    val sortedData = DataProcessor.sort(data)

    new DefaultWriter(outputDir, sortedData).write()
    val newReader = new DefaultReader(outputDir)
    val newData = newReader.data
    new DefaultReader(inputSortedDir).data.zip(newData).foreach { case (expected, actual) => 
      assert(expected.key === actual.key && expected.value === actual.value)
    }

    assert {
      newData.length === (new DefaultReader(inputSortedDir).data.length)
    }
  }
}

class ReaderIteratorSuite extends AnyFunSuite {

  class DefaultReader(val inputDir: String) extends DataReader

  test("DatumFileIterator should read all data correctly from a file.") {
    val inputDir = "src/test/resources/bin_data"
    val readerIterator = new DatumFileIterator(inputDir)
    val reader = new DefaultReader(inputDir)

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

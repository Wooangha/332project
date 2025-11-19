package worker.io

import java.nio.file.{Paths, Files, Path}
import java.io.RandomAccessFile

import scala.concurrent.blocking

import worker.io.DatumParser.parse
import common.{Datum, Key, Value}


trait FileReader[T] {
  val inputDir: String
  val parser: Parser[T]

  lazy val contents: Seq[T] = {
    // val bytes = blocking { Files.readAllBytes(Paths.get(inputDir)) }
    val bytes = Files.readAllBytes(Paths.get(inputDir))
    bytes.sliding(parser.dataSize, parser.dataSize).map(parser.parse(_)).toSeq
  }
}


trait FileIterator[T] extends Iterator[T] with AutoCloseable {
  val inputDir: String
  val parser: Parser[T]

  private[this] var start: Long = 0L
  private[this] lazy val raf = new RandomAccessFile(inputDir, "r")
  private[this] var nextValue: Option[T] = None

  private[this] def loadNext(): Unit = {
    val remaining = raf.length() - start
    if (remaining < parser.dataSize) {
      nextValue = None
    } else {
      val buf = new Array[Byte](parser.dataSize)

      raf.seek(start)
      val actuallyRead = raf.read(buf)
      start += actuallyRead

      if (actuallyRead > 0) {
        nextValue = Some(parser.parse(buf))
      } else {
        nextValue = None
      }
    }
  }

  override def hasNext: Boolean = {
    if (nextValue.isEmpty) {
      loadNext()
    }
    nextValue.nonEmpty
  }

  override def next(): T = {
    if (nextValue.isEmpty) {
      loadNext()
    }
    val res = nextValue.getOrElse(throw new NoSuchElementException("no more data"))
    nextValue = None
    res
  }

  override def close(): Unit = raf.close()
}


trait FileWriter[T] {
  val outputDir: String
  val data: Seq[T]
  val parser: Parser[T]

  def write(): Unit = {
    val path = Paths.get(outputDir)
    Files.createDirectories(path.getParent)
    val bytes = data.foldRight(Vector[Byte]()){ (content, accum) => 
      parser.serialize(content).toVector ++ accum
    }.toArray
    // blocking { Files.write(path, bytes) }
    Files.write(path, bytes)
  }
}


class DatumFileReader(val inputDir: String) extends FileReader[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

class DatumFileWriter(val outputDir: String, val data: Seq[Datum]) extends FileWriter[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

class DatumFileIterator(val inputDir: String) extends FileIterator[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

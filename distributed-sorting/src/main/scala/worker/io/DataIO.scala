package worker.io

import java.nio.file.{Paths, Files, Path}
import java.io.{RandomAccessFile, ByteArrayOutputStream}

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
  val maxChunkSize: Int

  private[this] var start: Long = 0L
  private[this] lazy val raf = new RandomAccessFile(inputDir, "r")

  private[this] lazy val fileLength: Long = raf.length()
  private[this] lazy val recordSize = parser.dataSize
  private[this] var position: Long = 0L
  private[this] var nextValue: Option[T] = None

  private[this] var chunk: Array[Byte] = Array.emptyByteArray
  private[this] var chunkPosition: Int = 0

  private[this] def loadNext(): Unit = {
    if (position + recordSize > fileLength && chunkPosition >= chunk.length) {
      nextValue = None
    } else {
      if (chunkPosition >= chunk.length) {
        val bytesToRead = Math.min(maxChunkSize * recordSize, (fileLength - position).toInt)
        chunk = new Array[Byte](bytesToRead)
        raf.seek(position)
        val actuallyRead = raf.read(chunk)
        position += actuallyRead
        chunkPosition = 0
      }

      val buf = new Array[Byte](recordSize)
      Array.copy(chunk, chunkPosition, buf, 0, recordSize)
      chunkPosition += recordSize

      // raf.seek(start)
      // val actuallyRead = raf.read(buf)
      // start += actuallyRead

      nextValue = Some(parser.parse(buf))
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
    val baos = new ByteArrayOutputStream()
    data.foreach { t =>
      baos.write(parser.serialize(t).toArray)
    }
    Files.createDirectories(Paths.get(outputDir).getParent)
    blocking { Files.write(Paths.get(outputDir), baos.toByteArray) }
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
  override val maxChunkSize: Int = 10000
}

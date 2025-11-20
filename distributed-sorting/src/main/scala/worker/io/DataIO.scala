package worker.io

import java.nio.file.{Paths, Files, Path}
import java.io.{FileInputStream, BufferedInputStream, DataInputStream, EOFException, IOException, ByteArrayOutputStream}

import scala.concurrent.blocking

import worker.io.DatumParser.parse
import common.{Datum, Key, Value}


abstract class FileReader[T] {
  val inputDir: String
  val parser: Parser[T]

  lazy val contents: Seq[T] = {
    val bytes = Files.readAllBytes(Paths.get(inputDir))
    bytes.sliding(parser.dataSize, parser.dataSize).map(parser.parse(_)).toSeq
  }
}


abstract class FileIterator[T](
    val inputDir: String,
    val parser: Parser[T],
    val maxChunkSize: Int) extends Iterator[T] with AutoCloseable {

  private[this] lazy val bufferSize: Int = maxChunkSize * parser.dataSize
  private[this] lazy val fis = new FileInputStream(inputDir)
  private[this] lazy val bis = new BufferedInputStream(fis, bufferSize) 
  private[this] lazy val dis = new DataInputStream(bis)

  private[this] var nextItem: Option[T] = tryReadNext()

  private[this] def tryReadNext(): Option[T] = {
    try {
      val buf = new Array[Byte](parser.dataSize)
      dis.readFully(buf)
      Some(parser.parse(buf))
    } catch {
      case _: EOFException => None
      case _: IOException => None
    }
  }

  override def hasNext: Boolean = nextItem.isDefined

  override def next(): T = {
    val res = nextItem.getOrElse(throw new NoSuchElementException)
    nextItem = tryReadNext()
    res
  }

  override def close(): Unit = dis.close()
}



abstract class FileWriter[T] {
  val outputDir: String
  val data: Seq[T]
  val parser: Parser[T]

  def write(): Unit = {
    val baos = new ByteArrayOutputStream()
    data.foreach { t =>
      baos.write(parser.serialize(t).toArray)
    }
    Files.createDirectories(Paths.get(outputDir).getParent)
    val bytes = baos.toByteArray
    blocking { Files.write(Paths.get(outputDir), bytes) }
  }
}


class DatumFileReader(val inputDir: String) extends FileReader[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

class DatumFileWriter(val outputDir: String, val data: Seq[Datum]) extends FileWriter[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

class DatumFileIterator(inputDir: String) extends FileIterator[Datum](inputDir, DatumParser, 10000)

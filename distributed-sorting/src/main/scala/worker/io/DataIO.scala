package worker.io

import java.nio.file.{Paths, Files, Path}
import java.io.{FileInputStream, BufferedInputStream, DataInputStream, EOFException, IOException, ByteArrayOutputStream}
import java.io.{FileOutputStream, BufferedOutputStream}

import scala.concurrent.blocking

import worker.io.DatumParser.parse
import common.{Datum, Key, Value}


abstract class FileReader[T] {
  val inputDir: String
  val parser: Parser[T]

  lazy val  contents: Seq[T] = {
    val bytes = Files.readAllBytes(Paths.get(inputDir))
    bytes.sliding(parser.dataSize, parser.dataSize).map(parser.parse(_)).toSeq
  }
}

/**
  * File iterator to read data from file in chunks
  *
  * @param inputDir input file path
  * @param parser parser to parse data from bytes
  * @param maxChunkSize maximum number of items to read in one chunk
  */
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
      case e: IOException =>
        System.err.println(s"IOException while reading from $inputDir: " + e)
        None
    }
  }

  override def hasNext: Boolean = nextItem.isDefined

  override def next(): T = {
    val res = nextItem.getOrElse(throw new NoSuchElementException)
    nextItem = tryReadNext()
    res
  }

  override def close(): Unit = {
    // Explicitly close all streams in reverse order of creation
    try {
      dis.close()
    } finally {
      try {
        bis.close()
      } finally {
        fis.close()
      }
    }
  }
}



abstract class FileWriter[T] {
  val outputDir: String
  val data: Seq[T]
  val parser: Parser[T]

  def write(): Unit = {
    val path = Paths.get(outputDir)
    Files.createDirectories(path.getParent)
    val bos = new BufferedOutputStream(new FileOutputStream(path.toFile))
    try {
      data.foreach { t =>
        bos.write(parser.serialize(t).toArray)
      }
    } finally {
      bos.close()
    }
  }
}


class DatumFileReader(val inputDir: String) extends FileReader[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

class DatumFileWriter(val outputDir: String, val data: Seq[Datum]) extends FileWriter[Datum] {
  override val parser: Parser[Datum] = DatumParser
}

class DatumFileIterator(inputDir: String) extends FileIterator[Datum](inputDir, DatumParser, 100)

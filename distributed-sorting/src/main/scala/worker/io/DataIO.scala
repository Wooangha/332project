package worker.io

import scala.io.Source
import java.nio.file.{Paths, Files, Path}
import java.io.RandomAccessFile

object DataParse {
  def getList(s: Seq[Byte]): List[Datum] = {
    s.sliding(100, 100).map(_.splitAt(10)).map { case (key, value) =>
      Datum(Key(key.toVector), Value(value.toVector))
    }.toList
  }
  def data2Map(data: List[Datum]) = data.map(_.toTuple).toMap
  def getMap(s: Seq[Byte]): Map[Key, Value] = {
    s.sliding(100, 100).map(_.splitAt(10)).map { case (key, value) =>
      (Key(key.toVector), Value(value.toVector))
    }.toMap
  }
  
  def unparse(data: List[Datum]): Vector[Byte] = {
    def unparseDatum(datum: Datum): Vector[Byte] = datum match {
      case Datum(Key(key), Value(value)) => key ++ value
    }
    data.foldRight(Vector[Byte]()) { case (datum, accum) =>
      unparseDatum(datum) ++ accum
    }
  }
}

trait DataReader {
  val inputDir: String

  lazy val data: List[Datum] = {
    val bytes = Files.readAllBytes(Paths.get(inputDir))
    DataParse.getList(bytes)
  }
}

trait FileIterator[T] extends Iterator[T] with AutoCloseable {
  val inputDir: String
  val stepSize: Int
  def parse(buf: Array[Byte]): T

  private[this] var start: Long = 0L
  private[this] lazy val raf = new RandomAccessFile(inputDir, "r")
  private[this] var nextValue: Option[T] = None

  private[this] def loadNext(): Unit = {
    val remaining = raf.length() - start
    if (remaining < stepSize) {
      nextValue = None
    } else {
      val buf = new Array[Byte](stepSize)

      raf.seek(start)
      val actuallyRead = raf.read(buf)
      start += actuallyRead

      if (actuallyRead > 0) {
        nextValue = Some(parse(buf))
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

class DatumFileIterator(val inputDir: String) extends FileIterator[Datum] {
  override val stepSize: Int = 100
  override def parse(buf: Array[Byte]): Datum = DataParse.getList(buf).head
}

trait DataWriter {
  val outputDir: String
  val data: List[Datum]

  def write(): Unit = {
    Files.write(Paths.get(outputDir), DataParse.unparse(data).toArray)
  }
}

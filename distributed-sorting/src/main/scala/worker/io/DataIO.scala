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

trait DatumFileIterator extends Iterator[Datum] with AutoCloseable {
  val inputDir: String
  val STEP_SIZE: Int = 100

  private[this] var start: Long = 0L
  private[this] lazy val raf = new RandomAccessFile(inputDir, "r")
  private[this] var nextDatum: Option[Datum] = None

  private[this] def loadNext(): Unit = {
    val remaining = raf.length() - start
    if (remaining < STEP_SIZE) {
      nextDatum = None
    } else {
      val buf = new Array[Byte](STEP_SIZE)

      raf.seek(start)
      val actuallyRead = raf.read(buf)
      start += actuallyRead

      if (actuallyRead > 0) {
        nextDatum = Some(DataParse.getList(buf).head)
      } else {
        nextDatum = None
      }
    }
  }

  override def hasNext: Boolean = {
    if (nextDatum.isEmpty) {
      loadNext()
    }
    nextDatum.nonEmpty
  }

  override def next(): Datum = {
    if (nextDatum.isEmpty) {
      loadNext()
    }
    val res = nextDatum.getOrElse(throw new NoSuchElementException("no more data"))
    nextDatum = None
    res
  }

  override def close(): Unit = raf.close()
}

trait DataWriter {
  val outputDir: String
  val data: List[Datum]

  def write(): Unit = {
    Files.write(Paths.get(outputDir), DataParse.unparse(data).toArray)
  }
}

package common

import worker.io.{DatumParser, DatumFileReader, DatumFileWriter}

case class Key(key: Vector[Byte]) extends Ordered[Key] {
  override def compare(that: Key): Int = {
    this.key.zip(that.key).foldLeft(0) { case (prev, (a, b)) => 
      val (unsignedA, unsignedB) = (a & 0xFF, b & 0xFF)
      if (prev != 0) {
        prev 
      } else if (unsignedA < unsignedB) {
        -1
      } else if (unsignedA > unsignedB) {
        1
      } else {
        0
      }
    }
  }
  override def toString(): String = {
    s"Key(${key.map(b => f"${b & 0xFF}%02X").mkString(", ")})"
  }
}

object Key {
  val min = Key(Vector.fill(10)(0.toByte))
  val max = Key(Vector.fill(10)(255.toByte))
}

case class Value(value: Vector[Byte]) {
  override def toString(): String = {
    s"Value(${value.map(b => f"${b & 0xFF}%02X").mkString(", ")})"
  }
}

case class Datum(key: Key, value: Value) {
  def toTuple: Tuple2[Key, Value] = (key, value)
  override def toString(): String = {
    s"Datum(${key.toString()}, ${value.toString()})"
  }
}

class Data(val data: Vector[Datum]) {
  type Ip = String

  def sampling(sampleSize: Int): Vector[Key] = {
    val step = Math.max(1, data.length / sampleSize)
    for {
      (datum, idx) <- data.zipWithIndex
      if idx % step == 0
    } yield datum.key
  }

  def partitioning(partition: Key => Ip): Map[Ip, Data] = {
    data.groupBy(datum => partition(datum.key)).view.mapValues(d => new Data(d)).toMap
  }

  def sort(): Data = new Data(data.sortBy(_.key))

  def appended(datum: Datum): Data = new Data(data.appended(datum))

  def ++(other: Data): Data = new Data(data ++ other.data)

  def save(dir: String): Unit = {
    new DatumFileWriter(dir, data).write()
  }

}

object Data {
  def fromFile(dataDir: String): Data = {
    new Data(new DatumFileReader(dataDir).contents.toVector)
  }

  def fromBytes(bytes: Seq[Byte]): Data = {
    val parser = DatumParser
    new Data(bytes.sliding(parser.dataSize, parser.dataSize).map(parser.parse).toVector)
  }
}

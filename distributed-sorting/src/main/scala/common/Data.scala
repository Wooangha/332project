package common

import java.util.Arrays

import worker.io.{DatumParser, DatumFileReader, DatumFileWriter}

case class Key(key: Array[Byte]) extends Ordered[Key] {
  override def compare(that: Key): Int = {
    val thisK = this.key
    val thatK = that.key
    var i = 0
    val len = thisK.length
    while (i < len) {
      val a = thisK(i) & 0xFF
      val b = thatK(i) & 0xFF
      if (a != b) {
        return a - b
      }
      i += 1
    }
    0
  }
  override def equals(obj: Any): Boolean = obj match {
    case that: Key => Arrays.equals(this.key, that.key)
    case _         => false
  }
  override def hashCode(): Int = Arrays.hashCode(key)
  def ===(other: Key): Boolean = {
    this.key.sameElements(other.key)
  }
  override def toString(): String = {
    s"Key(${key.map(b => f"${b & 0xFF}%02X").mkString(", ")})"
  }
}

object Key {
  val min = Key(Array.fill(10)(0.toByte))
  val max = Key(Array.fill(10)(255.toByte))
}

case class Value(value: Array[Byte]) {
  override def toString(): String = {
    s"Value(${value.map(b => f"${b & 0xFF}%02X").mkString(", ")})"
  }
  def ==(other: Value): Boolean = {
    this.value.sameElements(other.value)
  }
  def ===(other: Value): Boolean = {
    this.value.sameElements(other.value)
  }
}

case class Datum(key: Key, value: Value) {
  def toTuple: Tuple2[Key, Value] = (key, value)
  override def toString(): String = {
    s"Datum(${key.toString()}, ${value.toString()})"
  }
  def ==(other: Datum): Boolean = {
    this.key.key.sameElements(other.key.key) && this.value.value.sameElements(other.value.value)
  }
  def ===(other: Datum): Boolean = {
    this.key === other.key && this.value === other.value
  }
}

class Data(val data: Array[Datum]) {
  type Ip = String

  def sampling(sampleSize: Int): Array[Key] = {
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

  def ==(other: Data): Boolean = {
    if (this.data.length != other.data.length) return false
    for (i <- this.data.indices) {
      if (!(this.data(i) === other.data(i))) return false
    }
    true
  }
  def ===(other: Data): Boolean = {
    if (this.data.length != other.data.length) return false
    for (i <- this.data.indices) {
      if (!(this.data(i) === other.data(i))) return false
    }
    true
  }

}

object Data {
  def fromFile(dataDir: String): Data = {
    new Data(new DatumFileReader(dataDir).contents.toArray)
  }

  def fromBytes(bytes: Seq[Byte]): Data = {
    val parser = DatumParser
    new Data(bytes.sliding(parser.dataSize, parser.dataSize).map(parser.parse).toArray)
  }
}

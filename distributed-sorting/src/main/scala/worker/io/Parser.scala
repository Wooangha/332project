package worker.io

import common.{Datum, Key, Value}

trait Parser[T] {
  /** Byte size of the data */
  val dataSize: Int

  def parse(s: Seq[Byte]): T
  def serialize(t: T): Seq[Byte]
}


object DatumParser extends Parser[Datum] {
  override val dataSize: Int = 100

  override def parse(s: Seq[Byte]): Datum = {
    require(s.length == 100, "The Length of Bytes must be 100")

    s.splitAt(10) match { case (key, value) =>
      Datum(Key(key.toVector), Value(value.toVector))
    }
  }

  override def serialize(datum: Datum): Vector[Byte] = datum match {
    case Datum(key, value) => key.key ++ value.value
  }
}
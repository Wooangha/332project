package worker.io

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

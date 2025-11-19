package common

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

package common

import java.util.Arrays

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
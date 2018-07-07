package org.timchen.dslab5

import java.util.Date

class Key(c: String, t: Date) extends Ordered[Key] with java.io.Serializable{
  val name = c
  val time = t
  val id = (time.getTime / 1000 % 86400 + c(0) - 'A').toInt

  override def compare(k: Key): Int = {
    val f = time.compareTo(k.time)
    if (f != 0)
      f
    else
      name.compareTo(k.name)
  }

  override def equals(o: scala.Any): Boolean = {
    if (!o.isInstanceOf[Key])
      false
    else
      id == o.asInstanceOf[Key].id
  }

  override def hashCode(): Int = id
}
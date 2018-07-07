package org.timchen.dslab5

import java.text.SimpleDateFormat
import java.util.Date

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class Result (n: String, i: BigDecimal, e: BigDecimal, t: Date) extends java.io.Serializable{
  val name = n
  val income = i
  val expend = e
  val time = t

  override def toString: String ={
    val json = (("name" -> name)
      ~ ("income" -> income)
      ~ ("expend" -> expend)
      ~ ("time" -> new SimpleDateFormat("yyyy-MM-dd HH:mm").format(time)))
    return compact(render(json))
  }

}

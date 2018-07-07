package org.timchen.dslab5

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}

class Order (i: String, r: String, to: BigDecimal, t: Date){
  val initiator = i
  val recipient = r
  val turnover = to
  val time = t

  def this(jObject: JSONObject) = this(jObject.getString("src_name"),
    jObject.getString("dst_name"),
    BigDecimal(jObject.getBigDecimal("value").setScale(2)),
    jObject.getDate("time"))

  def this(jString: String) = this(JSON.parseObject(jString))
}

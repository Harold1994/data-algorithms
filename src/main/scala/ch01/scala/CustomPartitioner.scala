package ch01.scala

import org.apache.spark.Partitioner


class CustomPartitioner(partitons :Int) extends Partitioner{

  require(partitons >0, s"Number of partitons ($partitons) cannot be negative.")
  override def numPartitions: Int = partitons

  override def getPartition(key: Any): Int = key match {
    case (k:String, v:Int) => math.abs(k.hashCode % numPartitions)
    case null => 0
    case _ => math.abs(key.hashCode() % numPartitions)
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case h:CustomPartitioner => h.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode(): Int = numPartitions
}

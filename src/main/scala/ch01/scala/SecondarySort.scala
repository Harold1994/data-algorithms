package ch01.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Seq

object SecondarySort {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage <number-of-partitions> <input-path> <output-path>")
      sys.exit(1)
    }

    val partitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)

    val config = new SparkConf()
      .setMaster("local")
      .setAppName("SecondarySort")

    val sc = new SparkContext(config)
    val input = sc.textFile(inputPath)
    val valueToKey = input.map(x => {
      val line = x.split(",")
      ((line(0) + "-" + line(1) , line(2).toInt), line(2).toInt)
    }
    )

    implicit def tupleOrderingDesc = new Ordering[Tuple2[String, Int]] {
      override def compare(x: Tuple2[String, Int], y: Tuple2[String, Int]): Int = {
        if (y._1.compareTo(x._1) == 0)
          y._2.compareTo(x._2)
        else y._1.compareTo(x._1)
      }
    }

    val sorted = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions))
    val result = sorted.map {
      case (k,v) => (k._1, v)
    }
    result.saveAsTextFile(outputPath)
    sc.stop()
  }
}

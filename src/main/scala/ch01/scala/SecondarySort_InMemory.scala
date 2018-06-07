package ch01.scala

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySort_InMemory {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage <input-path> <output-path>")
      sys.exit(1)
    }

    val inpath = args(0)
    val outpath = args(1)
//    val inpath = "/Users/harold-mac/Documents/data-algorithms/input"
//    val outpath = "./out"
    val config = new SparkConf()
      .setAppName("SecondarySort_InMemory")
      .setMaster("local")
    val sc = new SparkContext(config)
    val input = sc.textFile(inpath)
    val tmp = input.map(line => {
      val x = line.split(",")
      (x(0),(x(1).toInt,x(2).toInt))
    })
    val output = tmp.groupByKey().mapValues(x => x.toList.sortWith((x, y) => x._1 > y._1))
    //或者
//    val output = tmp.groupByKey().map {
//      case (name, iter) => iter.toList.sortWith((x, y) => x._1 > y._1)
//    }
    output.saveAsTextFile(outpath)
  }
}

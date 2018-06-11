package ch03.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

object TopN {
  def main(args: Array[String]): Unit = {
//    if (args.size < 1) {
//      println("Usage : TopN <input>")
//      sys.exit(1)
//    }

    val sparkConf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //将N设为全局变量
    val N = sc.broadcast(10)
    val path = "file:///Users/harold-mac/Documents/data-algorithms/src/unput.csv"

    val input = sc.textFile(path)
    val pair = input.map(line => {
      var tokens = line.split(",")
      (tokens(2).toInt, tokens)
    })

    import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, Array[String]]
      itr.foreach { tuple => {
        sortedMap += tuple
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
          }
        }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, Array[String]].++(alltop10)
    val resultUsingMapPartiton = finaltop10.takeRight(N.value)

    //打印输出
    resultUsingMapPartiton.foreach({
      case (k, v) => println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    })

    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(N.value)
    moreConciseApproach.foreach {
      case (k, v) => println(s"$k \t ${v.flatten.mkString(",")}")
    }
    sc.stop()
  }
}

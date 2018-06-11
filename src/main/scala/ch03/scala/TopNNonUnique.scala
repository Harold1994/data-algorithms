package ch03.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

class TopNNonUnique {
  def main(args: Array[String]): Unit = {
    if (args.size < 1) {
      println("Usage: TopNNonUnique <input>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("TopNNonUnique")
    val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(2)
    val path = args(0)

    val input = sc.textFile(path)
    val kv = input.map(line => {
      val tokens = line.split(",")
      (tokens(0), tokens(1).toInt)
    })

    val uniqueKeys = kv.reduceByKey(_ + _)
    import Ordering.Implicits._
    val partitions = uniqueKeys.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach { tuple => {
        sortedMap += tuple.swap
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, String].++(alltop10)
    val resultUsingMapPartiton = finaltop10.takeRight(N.value)

    resultUsingMapPartiton.foreach {
      case (k, v) => println(s"$k \t ${v.mkString(",")}")
    }

    //更简 洁的一种方式
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => (a + b)
    val moreConciseApproach = kv.combineByKey(createCombiner, mergeValue, mergeValue)
      .map(_.swap)
      .groupByKey()
      .sortByKey(false)
      .take(N.value)

    //Prints result (top 10) on the console
    moreConciseApproach.foreach {
      case (k, v) => println(s"$k \t ${v.mkString(",")}")
    }
    sc.stop()
  }
}

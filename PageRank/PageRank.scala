import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.HashMap

object PageRank {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: PageRank InputFile Term")
    }
    val initialValue = 10
    val iterations = args(1)
    val alpha = 0.25
    val output = args(2)
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark PageRank").setMaster("local"))
    //val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    val file = sc.textFile(args(0))

    var flights =file.map(x=>(x.split(",")(1),x.split(",")(4)))
    val header = flights.first
    flights = flights.filter(x => x != header)
    val originId = flights.map{case (x,y) => x}.distinct().collect().toList
    val destinationId = flights.map{case (x,y) => y}.distinct().collect().toList

    val airports = originId.union(destinationId.filter(x => !originId.contains(x)))
    val outLinksMap = scala.collection.mutable.Map[String,Double]()
    val outgoing=flights.groupByKey()
    val nes=outgoing.map(x=>(x._1,x._2.size)).collect()

    for ((k,v)<- nes) outLinksMap.put(k,v)
    ////////////////
    var resultMap = scala.collection.mutable.Map[String,Double]()

    ///////////////

    var rankMap = scala.collection.mutable.Map[String,Double]()
    for (key<- airports) rankMap.put(key,initialValue)

    for(iteration <- iterations)
    {
      ///////////////loop://////////////////
      for (key <- airports) resultMap.put(key, 0)

      for (key <- airports) rankMap.put(key, rankMap.get(key).getOrElse(Double).asInstanceOf[Double] / outLinksMap.get(key).getOrElse(Double).asInstanceOf[Double])

      for ((k, v) <- flights.collect().toList) resultMap.put(v, resultMap.get(v).getOrElse(Double).asInstanceOf[Double] + rankMap.get(k).getOrElse(Double).asInstanceOf[Double])
      rankMap = resultMap
    }
    //////end of loop/////

    for((key,value) <- resultMap) {
      resultMap.put(key, (alpha/airports.size) + (1 - alpha) * value)
    }

    val result = resultMap.toSeq.sortBy(-_._2)
    sc.parallelize(result).saveAsTextFile(output + "/output")
  }
}

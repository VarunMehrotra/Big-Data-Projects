import org.apache.spark.{SparkConf, SparkContext}

object MoviePlot {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: MoviePlot InputFile Term")
    }

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    //val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    val stopWord = "'tis,'twas,a,able,about,across,after,ain't,all,almost,also,am,among,an,and,any,are,aren't,as,at,be,because,been,but,by,can,can't,cannot,could,could've,couldn't,dear,did,didn't,do,does,doesn't,don't,either,else,ever,every,for,from,get,got,had,has,hasn't,have,he,he'd,he'll,he's,her,hers,him,his,how,how'd,how'll,how's,however,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,it's,its,just,least,let,like,likely,may,me,might,might've,mightn't,most,must,must've,mustn't,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,shan't,she,she'd,she'll,she's,should,should've,shouldn't,since,so,some,than,that,that'll,that's,the,their,them,then,there,there's,these,they,they'd,they'll,they're,they've,this,tis,to,too,twas,us,wants,was,wasn't,we,we'd,we'll,we're,were,weren't,what,what'd,what's,when,when,when'd,when'll,when's,where,where'd,where'll,where's,which,while,who,who'd,who'll,who's,whom,why,why'd,why'll,why's,will,with,won't,would,would've,wouldn't,yet,you,you'd,you'll,you're,you've,your"

    val collectionBag = sc.textFile(args(0) + "/plot_summaries.txt").map(x => ((x.split("\t"))(0), x.split("\t")(1).replaceAll("[\\.$|,|;|']", "")))
    val N = collectionBag.count()
    val nI = collectionBag.filter{case(x,y) => y.contains(args(1))}.count()

    val tf = collectionBag.map{case (x,y) => (x,y.split(" ").filter(x => !stopWord.contains(x)).filter(value => value == args(1)).length)}


    val tfidf = tf.map{case(x,y) => (x, y * scala.math.log10(N/nI))}

    val movies = sc.textFile(args(0) + "/movie.metadata.tsv").map(x => (x.split("\t")(0), x.split("\t")(2)))

    val joinedTable = movies.join(tfidf)
    val result = joinedTable.map{case (x,(y,z)) => (y,z)}.sortBy(-_._2)

    result.saveAsTextFile(args(0) + "/output")
  }

}

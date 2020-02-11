import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank1 {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10
	
        val conf = new SparkConf()
            .setAppName("PageRank1")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val outlinks = sc
            .textFile(links_file, num_partitions)
            .map(remove_punctuation)
	    .map(line => line.split("\\s+"))
	    .map {
	    	 case Array(head, tail @ _*) => (head, tail)
	    }
	    .flatMap {
	    	 case (key, values) => values.map(x => (key.toLong, x.toLong))
	    }



        val titles = sc
	    .textFile(titles_file, num_partitions)
	    .zipWithIndex()
	    .map(x => (x._2 + 1, x._1))

	// src -> #src_out
	val src_srcCount = outlinks.map(x => (x._1, 1)).reduceByKey(_ + _)

	// tar, (src, #src_out)
	val tar_src_srcCount = outlinks.leftOuterJoin(src_srcCount)


        /* PageRank */
	val N: Long = titles.count
	val initWeight: Double = 100.0 / N
	val d: Double = 0.85
	val constant: Double = (1 - d) / N * 100
	var res = titles.map( x => (x._1,  initWeight))



        for (i <- 1 to iters) {
	    
	    // x, pr(y)/#out(y)
	    var y_score = tar_src_srcCount
            		  .leftOuterJoin(res)
			  // y, (x, #out(y), pr(y))
			  .map(x => (x._1, (x._2._1._1 , x._2._1._2.get, x._2._2.get)))
            		  .map(x => (x._2._1, x._2._3/x._2._2))

	    val x_yscore = titles
            .map( x => (x._1, 0.0))
            .union(y_score)

	    res = x_yscore.reduceByKey( (a, b) => (a + b) ).map(x => (x._1, constant + x._2 * d))
	    // res.collect().foreach(println)
	}
        
        
	val s = res.map(_._2).sum
	// take 10 or all?
	res = res.map(x => (x._1, x._2 / s * 100))
	val f = res.leftOuterJoin(titles).map(x => (x._1, (x._2._2, x._2._1)))
	
	// println("[ PageRanks ]")
	// f.sortBy(_._2._2, false).take(10).foreach(println)
	f.takeOrdered(10)(Ordering[Double].reverse.on(x =>x._2._2)).map(x => (x._1, (x._2._1.get, x._2._2))).foreach(println)
 
    }

    def remove_punctuation(line: String): String = {
    	line.replaceAll("""[\p{Punct}]""", " ")
    }
}

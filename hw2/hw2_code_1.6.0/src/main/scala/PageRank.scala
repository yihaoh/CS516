import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {

    	/*
    	val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
	*/

	val MASTER_ADDRESS = "ec2-34-232-77-236.compute-1.amazonaws.com"
	val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
	val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
	val INPUT_DIR = HDFS_MASTER + "/ECE516/input"
	val OUTPUT_DIR = HDFS_MASTER + "/ECE516/output"


	val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster(SPARK_MASTER)

        val sc = new SparkContext(conf)

        val outlinks = sc
            .textFile(INPUT_DIR + "/links-simple-sorted.txt", num_partitions)
            .map(remove_punctuation)
	    .map(line => line.split("\\s+"))
	    .map {
	    	 case Array(head, tail @ _*) => (head, tail)
	    }
	    .flatMap {
	    	 case (key, values) => values.map(x => (key.toLong, x.toLong))
	    }

        val titles = sc
	    .textFile(INPUT_DIR + "/titles-sorted.txt", num_partitions)
	    .zipWithIndex()
	    .map(x => (x._2 + 1, x._1))

	// src, #src_out
	val src_srcCount = outlinks.map(x => (x._1, 1)).reduceByKey(_ + _)

	// tar, (src, #src_out)
	val tar_src_srcCount = outlinks.leftOuterJoin(src_srcCount)


        /* PageRank */
	val initWeight: Double = 100.0 / titles.count
	val d: Double = 0.85
	val constant: Double = (1 - d) * 100.0 / titles.count
	var res = titles.map( x => (x._1,  initWeight))


        for (i <- 1 to iters) {
	    
	    // tar, pr(src)/#out(src)
	    var tar_weight = tar_src_srcCount
            		  .leftOuterJoin(res)
			  .map(x => (x._1, (x._2._1._1 , x._2._1._2.get, x._2._2.get)))
            		  .map(x => (x._2._1, x._2._3/x._2._2))

	    val src_tar_weight = titles
            		   .map( x => (x._1, 0.0))
            		   .union(tar_weight)

	    // page, weight
	    res = src_tar_weight.reduceByKey(_ + _).map(x => (x._1, constant + x._2 * d))
	    // res.collect().foreach(println)
	}
        
        
	val s = res.map(_._2).sum
	res = res.map(x => (x._1, x._2 / s * 100))
	val f = res.leftOuterJoin(titles).map(x => (x._1, (x._2._2, x._2._1)))

	sc.parallelize(f.takeOrdered(10)(Ordering[Double].reverse.on(x =>x._2._2)).map(x => (x._1, (x._2._1.get, x._2._2)))).saveAsTextFile(OUTPUT_DIR + "/PageRank")
	
	// sc.parallelize(f.sortBy(_._2._2, false).take(10)).saveAsTextFile(OUTPUT_DIR + "/PageRank")

    }

    def remove_punctuation(line: String): String = {
    	line.replaceAll("""[\p{Punct}]""", " ")
    }
}

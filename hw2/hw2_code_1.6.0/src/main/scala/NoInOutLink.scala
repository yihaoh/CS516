import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
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
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster(SPARK_MASTER)
            // .set("spark.driver.memory", "1g")
            // .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

	/* List all links */
        val links = sc
            .textFile(INPUT_DIR + "/links-simple-sorted.txt", num_partitions)
	    .map(remove_punctuation)
	    .map(line => line.split("\\s+"))
	    .map {
	    	 case Array(head, tail @ _*) => (head, tail)
	    }
	    .flatMap{
		case (key, values) => values.map((key.toLong, _))
	    }
	    .map(x => (x._1.toLong, x._2.toLong))
	// links.foreach(println)


	/* Get Titles */
        val titles = sc
            .textFile(INPUT_DIR + "/titles-sorted.txt", num_partitions)
	    .zipWithIndex()
	    .map{
		case (v, ind) => ((ind + 1).toLong, v)
	    }
	//titles.foreach(println)


        /* No Outlinks */
        val no_outlinks = titles.subtractByKey(links)
        println("[ NO OUTLINKS ]")
	sc.parallelize(no_outlinks.sortByKey(true).take(10)).saveAsTextFile(OUTPUT_DIR + "/NoOutLink")
	//foreach(println)
	

        /* No Inlinks */
        val no_inlinks = titles.subtractByKey(
	    	       	 links
	    	       	 .map( x => (x._2.toLong, x._1.toLong))
	    	       	 )
        println("\n[ NO INLINKS ]")
	sc.parallelize(no_inlinks.sortByKey(true).take(10)).saveAsTextFile(OUTPUT_DIR + "/NoInLink")
	//foreach(println)

	// TODO
    }

    def remove_punctuation(line: String): String = {
    	line.replaceAll("""[\p{Punct}]""", " ")
    }
}

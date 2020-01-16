import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
      
        val MASTER_ADDRESS = "ec2-3-228-217-9.compute-1.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/hw2/input"
        val num_partitions = 10
        

        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        
        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster(SPARK_MASTER)


        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions) //RDD[Array[T]]
            // TODO
        val links_lines = links.flatMap(make_pair) 
        val reverseLinks = links_lines.map(word => (word._2, word._1))
        
        val titles = sc
            .textFile(titles_file, num_partitions) //RDD[Array[T]]
            // TODO
        val titlesInd = titles.zipWithIndex()
            .map(word => (word._2 + 1, word._1))
        
        /* No Outlinks */
        val no_outlinks = titlesInd
            .leftOuterJoin(links_lines)
            .map(flat).filter(word => word._3 == None)
            .map(word =>(word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word => word._1))
        println("[ NO OUTLINKS ]")
        
        no_outlinks.foreach(println)
        
        // TODO

        /* No Inlinks */
        val no_inlinks = titlesInd
            .leftOuterJoin(reverseLinks)
            .map(flat).filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word => word._1))
        println("\n[ NO INLINKS ]")
        no_inlinks.foreach(println)
        // TODO
    }


    def make_pair(line : String): Array[(Long, Long)] = {
      val arr =  line.split(":") 
      val list = arr(1).split(" ").filter(word => word != "")
      val num : Int = list.length
      val res_line : Array[(Long, Long)] = new Array[(Long, Long)](num)
      for (i <- 0 to (num - 1)) {
        res_line(i) = (arr(0).toLong, list(i).toLong)
      }
      res_line     
    }
    def flat(line : (Long, (String, Option[Long]))) : (Long, String, Option[Long])= {
      val pair = line._2
      val b = pair._1//name
      val c = pair._2
      val res: (Long, String, Option[Long]) = (line._1, b, c)
      res
    }
}

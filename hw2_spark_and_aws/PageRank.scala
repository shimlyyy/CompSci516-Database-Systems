import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
      
        val MASTER_ADDRESS = "ec2-3-228-217-9.compute-1.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/hw2/input"
        
        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster(SPARK_MASTER)

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions).flatMap(make_pair)
            // TODO
        val links_map = links.groupByKey().mapValues(value => value.size) // {(2,1), }

      
        val titles = sc
            .textFile(titles_file, num_partitions)
        //title with index
         val titlesInd = titles.zipWithIndex().map(word => (word._2 + 1, word._1))
        
        val d: Double = 0.85
        val N = titles.count()
        val num : Double = (1 - d) * 100.0 / N       
        val rank : Double = 100.0/ N
        // leftOuterJoin generate none
        // total_page's format ((index,(name, outlinks, rank)) and initialized rank is 100.0/N
        var total_page = titlesInd.leftOuterJoin(links_map).map(flat1).map(word => (word._1, (word._2, word._3, rank)))

        
         /* PageRank */
        for (i<-1 to iters) {
           // contribs' format (index, the rank to its iter)
           val contribs = total_page.join(links).map(word => (word._2._2, word._2._1._3/ word._2._1._2))
                      .reduceByKey((a, b) => (a + b))
            total_page = total_page.leftOuterJoin(contribs).map(replace).map(word => (word._1, (word._2._1, word._2._2, num + d * word._2._3)))



        }
          println("[ PageRanks ]")
       
         // extract_page's format (index, name, rank)
         var extract_page = total_page.map(word => (word._1, word._2._1, word._2._3))
        
         
         val total_sum = extract_page.map(word =>(1, word._3)).reduceByKey((a,b) => a + b).first()._2
         
         extract_page.map(word => (word._1, word._2, word._3 * 100 / total_sum))
                     .takeOrdered(10)(Ordering[Double].reverse.on(word =>word._3))
                     .foreach(println)
         

        
    }
    
      
        def replace(line : (Long,((String, Int, Double), Option[Double]))): (Long, (String, Int, Double)) = {
           val index = line._1
           val name = line._2._1._1
           val out = line._2._1._2
           var new_rank = 0.0
           if (line._2._2 != None) {
             new_rank = line._2._2.get
           }
           val count:(Long, (String, Int, Double)) = (index, (name, out, new_rank))
           count
           
        }
     
        def flat1(line: (Long, (String, Option[Int]))): (Long, String, Int) = {
          val index = line._1
          val name = line._2._1
          val input = line._2._2
          var out = 0 
          if (input != None) {
            out = input.get
          }
          val count:(Long, String, Int) = (index, name, out)
          count
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
}

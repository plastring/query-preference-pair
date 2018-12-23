import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object PreferencePair {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd = sc.textFile("sample.txt")
//    rdd.foreach(println)

    val tempRdd = rdd.map(x => (x.split('|')(0), x.split('|')(2), x.split('|')(3), x.split('|')(4)))
      .groupBy(_._1)
      .map(x => (x._1, x._2.map(xx => (xx._2, xx._3, xx._4))))

    tempRdd.first()._2.foreach(println)
    tempRdd.foreach(x => getPair(x._1, x._2.toList))
  }

  def getPair(query: String, appList: List[Tuple3[String, String, String]]): List[Tuple2[String, String]] = {
    var showArray: Array[String] = new Array[String](15)
    var downloadArray: List[String] = List()
    var switch_flg = 0
    for(i <- 0 until appList.length) {
      if("61".equals(appList(i)._2) && switch_flg == 0) {
        showArray(appList(i)._1.toInt - 1) = appList(i)._3
        if(appList(i)._1.toInt == 15) {
          switch_flg = 1
        }
      } else if("11".equals(appList(i)._2)) {
        downloadArray = downloadArray :+ appList(i)._1
      } else if(!downloadArray.isEmpty){
        // TODO build pairs
        println(downloadArray.sortBy(x => x).reverse)
        downloadArray = List()
      }

      if((i == appList.length -1) && !downloadArray.isEmpty) {
        // TODO build pairs
        println(downloadArray.sortBy(x => x).reverse)
        downloadArray = List()
      }
    }
    return null
  }

}
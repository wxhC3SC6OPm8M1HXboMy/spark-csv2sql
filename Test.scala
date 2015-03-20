/**
 * Created by diego on 11/21/14.
 */

import org.apache.spark.{SparkContext,SparkConf}

import util.ReadCsv

/*
main code
 */

object Test {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("onvia").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    /*
    load csv files as tables
     */

    util.Files.File.values.foreach(v => {
      ReadCsv.readCsv(v,sc,sqlContext)
    })
    
    sc.stop()
  }
}

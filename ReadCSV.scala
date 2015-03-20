package util

/**
 * Created by diego on 11/23/14.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql._

object ReadCsv {

  /*
    load the csv file as table
   */

  def readCsv(params: Map[String,Any], sc: SparkContext, sqlContext: SQLContext): Unit = {

    // extract relevant values
    val fileName = params("file").asInstanceOf[String]
    val tableName = params("table").asInstanceOf[String]
    val formatting = params("formatting").asInstanceOf[Map[String,Map[String,Any]]]
    val delimiter = params("delimiter").asInstanceOf[String]

    val file = scala.io.Source.fromFile(fileName)

    // find header
    val header = file.getLines.take(1)
    val headerString = header.mkString("")

    // remove header
    val thisRDD = sc.textFile(fileName).mapPartitions(iterator => {
      val head = iterator.next()
      if (head == headerString) iterator else Iterator(head) ++ iterator
    }).map(_.split(delimiter,-1))

    // generate the schema based on the header
    val schema =
      StructType(
        headerString.split(",").map(fieldName => {
          val colName = fieldName.stripPrefix("\"").stripSuffix("\"").trim()
          if(formatting.contains(colName))
            StructField(colName,formatting(colName)("type").asInstanceOf[DataType],true)
          else
            StructField(colName, StringType, true)
        })
      )

    file.close()

    // convert records of the RDD to Rows
    val rowRDD = thisRDD.map(p => {
      p(0) = p(0).stripPrefix("\"")
      p(p.length - 1) = p(p.length - 1).stripSuffix("\"")

      // to capture columns that are not string
      // toPass is the final array passed to Row
      val toPass:Array[Any] = new Array[Any](p.length)
      Array.copy(p,0,toPass,0,p.length)
      // for each exception
      formatting.foreach{case(key,value) => {
        // get the formatting function
        val func = value("function").asInstanceOf[String => Any]
        val idx = value("index").asInstanceOf[Integer]
        // call the formatting function and update toPass
        toPass(idx) = func(p(idx))
      }}

      Row(toPass:_*)
    })

    // apply the schema to the RDD
    val thisSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // register the SchemaRDD as a table
    thisSchemaRDD.registerTempTable(tableName)
  }

}

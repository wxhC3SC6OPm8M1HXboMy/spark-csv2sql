package util

/**
 * Created by diego on 11/24/14.
 * 
 * Each csv file must have a header; the names in the schema match the column names listed in the header
 */

import org.apache.spark.sql._

object Files {

  /*
  * an example of a formatter function; it takes a string from the csv file, and it returns the coresponding schemaRDD object
  * the example gives Timestamp data type from a string
  */

  private val formatDateWithHour = ((field: String) => {
    val formatter = new java.text.SimpleDateFormat("MMM dd, yyyy, 'Hour' kk")
    formatter.setTimeZone(java.util.TimeZone.getTimeZone(Misc.TimeZone));
    new java.sql.Timestamp(formatter.parse(field).getTime())
  })

  // input files and formatting
  private val BasePath = "/Users/diego/Box Sync/Data/Onvia/project data/"

  /*
  * list here one entry per file
  */

  val File = Map(
    "file1" -> Map(
      "file" -> (BasePath + "file1.csv"), // name of the csv file
      "delimiter" -> "\",\"", 
      "table" -> "table1", // name of the table
      "formatting" -> Map() // if no special formatting is listed, each column is loaded as string
    ),
    "file2" -> Map(
      "file" -> (BasePath + "file2.csv"), // name of the csv file
      "delimiter" -> "\",\"",
      "table" -> "table2",
      // special formatting for columns
      "formatting" -> Map(
        "ColName" -> Map(  // name of the column in the table; it must be listed in the header of the file 
          "index" -> 2,   // the index of the column in the csv file
          "type" -> TimestampType, // schemaRDD type
          "function" -> formatDateWithHour // the function to be called to transform the string from the csv file into the schema data type
        )
      )
    )
  )
}

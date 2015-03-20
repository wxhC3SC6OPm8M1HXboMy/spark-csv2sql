Small utility for quickly loading csv files as schemaRDD. 
Each loaded csv file must have a header. The names from the header match the names in the schema. 
Formatting of a value from a csv file can be customized, i.e., the utility accepts custom formatter functions. 

To use the utility, import ReadCsv.scala.
Then customize Files.scala to list your files one by one. In this object, you can specify special formatting functions for values, provide the name of table, etc. 

Test.scala shows how to use the objects. 

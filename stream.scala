import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File
import java.io


object stream {
  def main(args: Array[String]) : Unit= {

    println("++++++++++++++++++++++   "+args(0)+"     ++++++++++++++++++++++++++")
    // initialise spark context
    val conf = new SparkConf().setAppName(stream.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).config("spark.master", "local").getOrCreate()

    val userSchema = new StructType().add("Msisdn", "integer")
      .add("Service_Identifier", "integer")
      .add("Traffic_Volume", "integer").add("Timestamp", "String")

    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv(args(1))

    val csvDF2 = csvDF.withColumn("Hour",csvDF("Timestamp").substr(9,2)).withColumn("Date",csvDF("Timestamp").substr(0,8))

    val segmentSchema=new StructType().add("dial", "integer")
    val segment= spark.read.schema(segmentSchema).csv(args(0)+"/SEGMENT.csv")


    val RulesSchema=new StructType().add("app_id", "integer").add("app_name","String")
      .add("st_time","integer").add("end_time","integer").add("Total_Volume","integer")
    val Rules= spark.read.schema(RulesSchema).csv(args(0)+"/RULES.csv")


    val seg= csvDF2.join(segment, csvDF2("Msisdn") === segment("dial"))
    val rule = seg.join(Rules, seg("Service_Identifier") === Rules("app_id")).filter("Traffic_Volume>=Total_Volume").filter("Hour between st_time and end_time")

    val output=rule.select("Msisdn","Service_Identifier","Traffic_Volume","Timestamp","app_name").distinct()


    val outputfile = new io.File(args(2)+"/output")
      outputfile.mkdir()

    val chkpoint = new io.File(args(2)+"/checkpoint")
    chkpoint.mkdir()

    val outpath=outputfile.getAbsolutePath
    val chkpath=chkpoint.getAbsolutePath
    val query = (output
      .writeStream
      .format("csv")
      .option("format", "append")
      .option("path" , outpath)
      .option("checkpointLocation",chkpath)
      .partitionBy("app_name")
      .outputMode("append").start())

    query.awaitTermination
    spark.stop()

  }
}

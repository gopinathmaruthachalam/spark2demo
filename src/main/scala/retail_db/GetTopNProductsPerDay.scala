package bosch_test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import spark.implicits._

/**
  * Created by Gopinath.M on 10/10/2019.
  * Problem Statement: Fetch the columns where all the values in those columns are correctly formatted as per the requirement.
  * Consider we have the input file in DIR: "inputBaseDir"
  * We will save the output file in DIR: "outputBaseDir"
  * Creating Spark configuration and Spark Context
  * Reading data (from file system or database)
  * Row level transformations (filter, data cleansing and standardization)
  * Writing processed data (to file system or database) - Actions
  */

object Fileter_Col {
  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load
    val env = args(0)
    val envProps = props.getConfig(env)

    val spark = SparkSession.
    builder.
    appName("Daily Product Revenue using Data Frame Operations").
    master(envProps.getString("execution.mode")).
    getOrCreate

    spark.conf.set("spark.sql.shuffle.partitions", "2")   
 
  
    // Reading data (from file system or database)

    val inputBaseDir = envProps.getString("input.base.dir")
    val df = spark.read.csv(inputBaseDir + "phonenumbers").
      toDF("c1", "c2", "c3", "c4","c5")

    // Row level transformation
    // Check what are all column values are correctly formatted as per the requirement
 
    val df1 = df.select(df.columns
              .map(c=> df(c) rlike("\\(\\d{3}\\)-\\d{3}-\\d{4}|\\d{3}-\\d{3}-\\d{4}|\\d{3}-\\d{3}\\d{4}|\\d{3}\\d{3}\\d{4}")): _*)

    // df1 - use map to transform the data and type Cast
   
    val df2 = df1.select(df1.columns
                         .map(c => df1(c).cast("Int")): _*)
    val df5 = df2.select(df2.columns
                         .map(c => sum(c)): _*)

    // rename are transformed column names to original column values.
   
    val names = df.columns
    val df3 = df5.toDF(names: _*)

 
    // We will fetch the array of columns which contain all values in proper format. 
   
    val filterColumns = df3.columns
                        .map(c => (c, df3.select(c).first.getLong(0)))
                        .filter{case (c, v) => v == df.count}
                        .map{case (c, v) => c}
                        .toSeq

    // Save the files using "coalesce" to minimize the shuffle.
   val outputBaseDir = envProps.getString("output.base.dir")
   val result = df.select(filterColumns.map(c => col(c)): _*)   
        
           result.coalesce(1).saveAsTextFile("output.base.dir")
  }
}

 

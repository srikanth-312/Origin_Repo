package sparkpack

import org.apache.spark._
import org.apache.spark.sql.functions.broadcast
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import scala.io.Source._                //scala import statement 


object sparkobj {

	def main(args:Array[String]):Unit={



			val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example")
					.config("spark.some.config.option", "some-value")
					.getOrCreate()

					import spark.implicits._

					val df = spark.read.format("CSV").option("header","true").load("file:///C:/Users/DELL/OneDrive/Desktop/data/employee")
					df.show()
					
					df.createOrReplaceTempView("df")
					val df2 = spark.sql("select * from df where salary > 10000")
					df2.show()
					




























	}




}

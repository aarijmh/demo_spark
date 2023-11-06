package io.auton8.spark.utility;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {

	public static SparkSession getSparkSession() {
		return SparkSession.builder().appName("Customer Aggregation pipeline").master("local").getOrCreate();
	}
	
	public static void main(String[] args) {
		Dataset<Row> df = getSparkSession().read().option("header",true).option("delimiter", ",").csv("/home/hadoop/Downloads/test1.csv");
		df = df.withColumn("new_column", when(col("A").isNull(), lit("N")).otherwise(regexp_replace(col("A"),"^.*$", "A")));
	
		df.show();

	}

}

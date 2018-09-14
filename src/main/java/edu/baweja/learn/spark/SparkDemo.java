package edu.baweja.learn.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDemo {

	public static void main(String[] args) {
		// SparkSession is the entry point into Spark application
		SparkSession spark = SparkSession.builder()
				.appName("Spark Demo")
				.master("local")
				.getOrCreate();

		// Dataframe is an alias for Dataset<Row>
		// Dataframe represents Untyped API in Spark Sql
		Dataset<Row> df = spark.read()
				.json("/d:/data/data.json");

		// Dataset represtens Typed API in Spark SQL
		Dataset<Person> ds = df.as(Encoders.bean(Person.class));

		df.printSchema();
		ds.printSchema();

		System.out.println(ds.count());

		ds.select("fname", "lname")
				.show();

		ds.map((Person p) -> p.getId(), Encoders.LONG())
				.show();

		ds.filter((Person p) -> p.getFname()
				.startsWith("A"))
				.show();

		Dataset<String> fNamesInUpperCase = ds.map((Person p) -> p.getFname()
				.toUpperCase(), Encoders.STRING());
		fNamesInUpperCase.show();
	}

}

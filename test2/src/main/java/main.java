package Traffic.trends;


import org.apache.log4j.Logger;

import org.apache.log4j.Level;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;


public class main {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //Creating Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Traffic-Stops")
                .master("local")
                .getOrCreate();

        //Loading Training Data into
        Dataset<Row> slt = spark.read()
                .csv("/home/prateek/Downloads/MSACLEAN.csv");
        //delete null value rows
       // Dataset<Row> cleanData = slt;//filter((FilterFunction<Row>) row -> !row.anyNull());
        //cleanData.show(345);
        slt.createOrReplaceTempView("conf");
        Dataset<Row> sqlSearch1 = spark.sql("SELECT _c0 AS State, count(_c0) AS Number FROM conf GROUP BY _c0 ORDER BY Number DESC");
        //sqlSearch1 = slt.filter((FilterFunction<Row>) row -> !row.anyNull());
        //sqlSearch1.filter("_c4 != 'http://abc11.com/...%7C'");
        sqlSearch1.show(345);

        sqlSearch1.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .save("/home/prateek/state.csv");

        Dataset<Row> sqlSearch2 = spark.sql("SELECT substring(_c4,-4) AS Year, count(substring(_c4,-4)) AS Number FROM conf GROUP BY substring(_c4,-4) ORDER BY Number DESC");
        sqlSearch2.show(345);

        sqlSearch2.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .save("/home/prateek/year.csv");

        Dataset<Row> sqlSearch3 = spark.sql("SELECT _c3 AS Race, count(_c3) AS Number FROM conf GROUP BY _c3 ORDER BY Number DESC");
        sqlSearch3.show(345);

        sqlSearch3.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .save("/home/prateek/race.csv");
        Dataset<Row> sqlSearch4 = spark.sql("SELECT _c2 AS Sex, count(_c2) AS Number FROM conf GROUP BY _c2 ORDER BY Number DESC");
        sqlSearch4.show(345);

        sqlSearch4.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .save("/home/prateek/sex.csv");

    Dataset<Row> sqlSearch5 = spark.sql("SELECT floor(_c1/10.00)*10 AS Under, COUNT(*) AS COUNT FROM conf GROUP BY 1 ORDER BY 1  ");
        sqlSearch5.show(345);

        sqlSearch5.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .save("/home/prateek/age.csv");

    }
}
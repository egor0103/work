import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source.fromFile
import scala.util.Try
import java.net.URL
import java.net.URLDecoder.decode

object data_mart extends App{
  val spark = SparkSession
    .builder()
    .appName("data_mart")
    .getOrCreate()

  //cassandra
  val tableOpts = Map("table" -> "clients", "keyspace" -> "labdata")
  val clients = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(tableOpts)
    .load()

  //elastic
  val esOptions = Map(
      "es.nodes" -> "10.0.0.31:9200 ",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true")
  val shoplogs = spark
    .read
    .format("es")
    .options(esOptions)
    .load("visits")

  //hdfs
  val weblogs = spark.read
    .option("header", "true")
    .json("hdfs:///labs/laba03/weblogs.json")

  //postgresql
  val url = "jdbc:postgresql://10.0.0.31:5432/labdata?user=egor_makartsev&password=Zx3HQ7eO"
  val pgoptions = Map(
    "url" -> url,
    "dbtable" -> "domain_cats",
    "user" -> "egor_makartsev",
    "password" -> "Zx3HQ7eO",
    "driver" -> "org.postgresql.Driver")
  val webcats = spark
    .read
    .format("jdbc")
    .options(pgoptions)
    .load

  //подготовка источников
  //обработка clients
  val clients_finish = clients
    .withColumn("age_cat", when(col("age") >= 18 && col("age") <= 24, "18-24")
      .when(col("age") >= 25 && col("age") <= 34, "25-34")
      .when(col("age") >= 35 && col("age") <= 44, "35-44")
      .when(col("age") >= 45 && col("age") <= 54, "45-54")
      .when(col("age") >= 55, ">=55"))
    .select(clients("uid"), clients("gender"), col("age_cat"))

  //обработка shoplogs
  val shoplogs_finish = shoplogs
    .na.drop(Seq("uid"))
    .withColumn( "shop_category", regexp_replace(concat(lit("shop_"), lower(col("category"))), lit("-| ") , lit("_")))

  //обработка weblogs
  val decode_url = udf { (url: String) => Try(new URL(decode(url, "UTF-8")).getHost).toOption }
  val weblogs_finish = weblogs
    .withColumn("visit", explode(col("visits")))
    .withColumn("domain", callUDF("parse_url", col("visit.url"), lit("HOST")))
    .withColumn("domain", regexp_replace(col("domain"), lit("www\\."), lit("")))

  //обработка webcats
  val webсats_finish = webCats.withColumn("web_category",
      regexp_replace(concat(lit("web_"), lower(col("category"))), lit("-| ") , lit("_")))
      .repartition(col("web_category"))

  //сбор витрины
  val weblog_cat = weblogs_finish
    .join(broadcast(webсats_finish), Seq("domain"), "full")
    .groupBy(col("uid"))
    .pivot(col("web_category"))
    .agg(count(col("web_category")))

  val data_mart = clients_finish
    .select(col("uid"), col("gender"), col("age_cat"))
    .join(shoplogs_finish, Seq("uid"), "left")
    .groupBy(col("uid"), col("gender"), col("age_cat"))
    .pivot(col("shop_category"))
    .agg(count(col("shop_category")))
    .join(broadcast(weblog_cat), Seq("uid"), "inner")
    .drop("null")

  data_mart.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/egor_makartsev")
    .option("dbtable", "clients")
    .option("user", "egor_makartsev")
    .option("password", "Zx3HQ7eO")
    .option("driver", "org.postgresql.Driver")
    .save()
}

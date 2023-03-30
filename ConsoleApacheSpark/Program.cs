using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;


namespace ConsoleApacheSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            /*SparkSession spark = SparkSession
                .Builder()
                .Config("spark.driver.port", "4040")
                .AppName("Streaming example with a UDF")
                .GetOrCreate();
            
            DataFrame lines = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", "localhost:9092")
                .Option("subscribe", "messages")
                .Option("startingOffsets", "earliest")
                .Load();
            
            DataFrame words = lines.Select(Expr("CAST(value AS STRING)"));
            
            Func<Column, Column> udf = Udf<string, string[]>(
                str => str.Split(' '));
            
            DataFrame wordCounts = words
                .Select(Explode(udf(words["value"])).Alias("word"))
                .GroupBy("word")
                .Count();
            
            StreamingQuery query = wordCounts
                .WriteStream()
                .OutputMode("append")
                .Format("console")
                .Start();

            query.AwaitTermination();*/

            // Create a SparkSession
            /*var spark = SparkSession.Builder().GetOrCreate();

            // Define Kafka connection parameters
            var kafkaParams = new Dictionary<string, string>
            {
                {"bootstrap.servers", "localhost:9092"},
                {"group.id", "my-group"},
                {"auto.offset.reset", "earliest"}
            };

            // Create a DataFrame from Kafka using the KafkaConsumer class
            var df = spark
                .Read()
                .Format("kafka")
                .Options(kafkaParams)
                .Option("subscribe", "messages")
                .Option("startingOffsets", "earliest")
                .Load();

            foreach (var obj in df.Collect())
            {
                Console.WriteLine("test");
            }
            // Extract the message value from the DataFrame and print the contents
            /*df
                .SelectExpr("CAST(value AS STRING)")
                .WriteStream()
                .Foreach(new ForeachWriter<Row>((row, _) =>
                {
                    var message = row.GetAs<string>("value");
                    Console.WriteLine((string)message);
                    return true;
                }))
                .Start()
                .AwaitTermination();*/

            var spark = SparkSession.Builder()
                .Config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .AppName("TaxiDriverAnalysis").GetOrCreate();
            
            var taxiDriverData = spark.Read().Option("header", true).Csv("/Users/burmas3/Desktop/taxi_driver_data.csv");

            var taxiDriversExpiringIn2023 = taxiDriverData
                .Filter(Col("Expiration Date").EndsWith("23"))
                .Select(Col("Name").As("FirstName"), 
                    Col("License Number").As("LicenseNumber"),
                    Col("Expiration Date").As("ExpiryDate"))
                .ToJSON()
                .Collect().ToList();
            
            //These taxi drivers license will expire this year
            foreach (var driver in taxiDriversExpiringIn2023)
            {
                Console.WriteLine(driver);
            }
            
            var expiredTaxiDrivers = taxiDriverData
                .Filter(ToDate(Col("Expiration Date"), "MM/dd/yy")< CurrentDate())
                .Select(Col("Name"), Col("License Number"), Col("Expiration Date"))
                .ToJSON()
                .Collect().ToList();
            
            //These are expired Taxi drivers
            foreach (var driver in expiredTaxiDrivers)
            {
                Console.WriteLine(driver);
            }

            var taxiDriversGroupedByYear = taxiDriverData
                .Filter(Col("Expiration Date").EndsWith("2023"))
                .Union(taxiDriverData.Filter(Col("Expiration Date").EndsWith("2024")))
                .Union(taxiDriverData.Filter(Col("Expiration Date").EndsWith("2025")))
                .Union(taxiDriverData.Filter(Col("Expiration Date").EndsWith("2025")))
                .Select(Col("Name"), Col("Expiration Date")).ToDF("Expiration_Date", "Name").Collect().ToList()
                ;

            //taxiDriversGroupedByYear.Show();
            
            spark.Stop();
        }
    }
}
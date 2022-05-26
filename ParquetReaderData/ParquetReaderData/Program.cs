using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Running;
using MongoDB.Bson;
using MongoDB.Driver;
using Parquet;
using Parquet.Data;
using Perfolizer.Horology;

namespace ParquetReaderData
{
    

    class Program
    {
        static void CheckMongotime(List<GtmCalcResult> dataList){
            //Get connection to MongoDB
            MongoDB mongo = new MongoDB("mongo", "data2", dataList);
            mongo.Append();
            //Make operations
            Stopwatch sw = new Stopwatch();
            sw.Start();
            mongo.Find(); //выбрать все данные //mongo.FindAll(2);
            sw.Stop();
            Console.WriteLine($"Время получения всех данных {sw.ElapsedMilliseconds}" );
            sw.Start();
            mongo.FindSome(); //выбрать данные по условию //mongo.FindSome(2);
            sw.Stop();
            Console.WriteLine($"Время получения данных по условиям {sw.ElapsedMilliseconds}" );
            sw.Start();
            mongo.RewriteAll().GetAwaiter().GetResult(); //заменить все данные
            sw.Stop();
            Console.WriteLine($"Замена всех данных {sw.ElapsedMilliseconds}" );
            sw.Start();
            mongo.MakeSomeOptions().GetAwaiter().GetResult();//удалить+заменить+оставить
            sw.Stop();
            Console.WriteLine($"Удалить + заменить + остановить {sw.ElapsedMilliseconds}" );
        }

        public void Mongo()
        {
            MongoConnect connection = new MongoConnect("mongodb://localhost");
            connection.GetConnection("mongo", "data3");
            MongoData mongo = new MongoData(connection.Collection);
            
            
            
        }
        public void CheckClicktime(List<GtmCalcResult> dataList)
        {
            List<long> result = new List<long>();
        }
        
        public void CheckYDBtime(List<GtmCalcResult> dataList)
        {
            List<long> result = new List<long>();
        }
        
        static void Main(string[] args)
        {
            //Collect data with ParquetParser
            ParquetParser parser = new ParquetParser(@"C:\Users\Maslov Alexander\Desktop\test.parquet");
            parser.CollectData();
            
            //Append collected data to struct
            List<GtmCalcResult> DataList = parser.FillStruct();
            CheckMongotime(DataList);
            var result = BenchmarkRunner.Run<MongoDB>();
        }
        
    }
}

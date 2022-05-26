using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ParquetReaderData
{

    public class MongoDB
    {
        private string _connectionString;
        private MongoClient _client;
        private IMongoDatabase _database;
        private IMongoCollection<BsonDocument> _collection;
        private List<GtmCalcResult> _data;

        public MongoDB(string DBname, string CollectionName, List<GtmCalcResult> data, string connectionString = "mongodb://localhost")
        {
            //Connection to DB
            _connectionString = connectionString;
            _client = new MongoClient(_connectionString);
            _database = _client.GetDatabase(DBname);
            _collection = _database.GetCollection<BsonDocument>(CollectionName);
            _data = data;
        }
        public void Append()
        {
            SaveDocs().GetAwaiter().GetResult();
        }
        public void Find()
        {
            FindDocs(new BsonDocument()).GetAwaiter().GetResult();
        }

        public int FindSome()
        {
            BsonDocument filter1 = new BsonDocument("$and", new BsonArray{
                 
                new BsonDocument("CALENDAR_YEAR",2017),
                new BsonDocument("V_YEAR",new BsonDocument("$ne", 0)),
            });
            
            BsonDocument filter2 = new BsonDocument("$or", new BsonArray{
                 
                new BsonDocument("CALENDAR_YEAR",2017),
                new BsonDocument("CALENDAR_YEAR",2019),
                new BsonDocument("V_M01",new BsonDocument("$lt", 1)),
                new BsonDocument("V_M01",new BsonDocument("$gt", 200)),
                new BsonDocument("V_M02",new BsonDocument("$eq", 0)),
                new BsonDocument("V_YEAR",new BsonDocument("$ne", 0)),
            });
            FindDocs(filter1).GetAwaiter().GetResult();
            return 0;
        }
        [Benchmark(Description = "RewriteAll")]
        public async Task RewriteAll()
        {
            var filter = new BsonDocument();
            var update = Builders<BsonDocument>.Update.Set("V_YEAR", 2035);
            var result = await _collection.UpdateManyAsync(filter, update);
            Console.WriteLine($"Изменено: {result.ModifiedCount}");
        }
        [Benchmark(Description = "MongoOptional")]
        public async Task MakeSomeOptions()
        {
            //удалим половину данных
            BsonDocument filter = new BsonDocument("CALENDAR_YEAR", 2019);
            var result = await _collection.DeleteManyAsync(filter);
            Console.WriteLine($"Удалено: {result.DeletedCount}");
            //заменим часть данных 
            filter = new BsonDocument("CALENDAR_YEAR", 2018);
            var update = Builders<BsonDocument>.Update.Set("V_YEAR", 2036);
            var result2 = await _collection.UpdateManyAsync(filter, update);
            Console.WriteLine($"Изменено: {result2.ModifiedCount}");
        }
        [Benchmark(Description = "SaveData")]
        private async Task SaveDocs()
        {
            List<BsonDocument> elements = new List<BsonDocument>();
            foreach (var el in _data)
            {
                BsonDocument person1 = new BsonDocument 
                {
                    {"CALENDAR_YEAR", el.CalendarYear},
                    {"IS_ALLPERIOD", el.IsAllPeriod},
                    {"MEASURE_ID", el.MeasureId},
                    {"PEO_VERSION_ID", el.PeoVersionId},
                    {"PLANNING", el.PlanningObject },
                    {"V_M01", el.Month01Value},
                    {"V_M02", el.Month02Value},
                    {"V_M03", el.Month03Value},
                    {"V_M04", el.Month04Value},
                    {"V_M05", el.Month05Value},
                    {"V_M06", el.Month06Value},
                    {"V_M07", el.Month07Value},
                    {"V_M08", el.Month08Value},
                    {"V_M09", el.Month09Value},
                    {"V_M10", el.Month10Value},
                    {"V_M11", el.Month11Value},
                    {"V_M12", el.Month12Value},
                    {"V_YEAR", el.YearValue}
                };
                elements.Add(person1);
            }
            Console.WriteLine($"Добавлено {elements.Count}");
            await _collection.InsertManyAsync(elements);
        }
        
        // быстрый, но забивает всю бд сразу  в память
        private async Task FindDocs(BsonDocument filter)
        {
            List<BsonDocument> people = await _collection.Find(filter).ToListAsync();
            Console.WriteLine($"Найдено {people.Count}");
            
        }
        // экономный по памяти, но в 2 раза медленнее
        private async Task FindDocs2(BsonDocument filter)
        {
            using (var cursor = await _collection.FindAsync(filter))
            {
                while (await cursor.MoveNextAsync())
                {
                    var people = cursor.Current;
                }
                 
            }
        }
    }

    class ReadData
    {
        
    }
}
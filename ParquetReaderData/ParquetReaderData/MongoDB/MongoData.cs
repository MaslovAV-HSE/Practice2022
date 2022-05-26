using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ParquetReaderData
{
    public class MongoData
    {
        private readonly IMongoCollection<BsonDocument> _collection;

        public MongoData(IMongoCollection<BsonDocument> connection)
        {
            _collection = connection;
            
        }
        
        public void Append(List<GtmCalcResult> Data)
        {
            SaveDocs(Data).GetAwaiter().GetResult();
        }
        public void Delete(BsonDocument filter)
        { 
            DeleteDocs(filter).GetAwaiter().GetResult();
        }

        public void Find(BsonDocument filter)
        {
            FindDocs(filter).GetAwaiter().GetResult();
        }

        public void Rewrite(BsonDocument filter, string fild, double value)
        {
            RewriteDocs(filter,fild,value).GetAwaiter().GetResult();
        }
        
        private async Task SaveDocs(List<GtmCalcResult> _data)
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
        
        private async Task FindDocs(BsonDocument filter)
        {
            List<BsonDocument> people = await _collection.Find(filter).ToListAsync();
            Console.WriteLine($"Найдено {people.Count}");
            
        }
        private async Task DeleteDocs(BsonDocument filter)
        {
            var result = await _collection.DeleteManyAsync(filter);
            Console.WriteLine($"Удалено: {result.DeletedCount}");
        }
        public async Task RewriteDocs(BsonDocument filter, string fild, double value)
        {
            var update = Builders<BsonDocument>.Update.Set(fild, value);
            var result = await _collection.UpdateManyAsync(filter, update);
            Console.WriteLine($"Изменено: {result.ModifiedCount}");
        }
        
    }
}
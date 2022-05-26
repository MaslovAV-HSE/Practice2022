using MongoDB.Bson;
using MongoDB.Driver;

namespace ParquetReaderData
{
    public class  MongoConnect : IDataBaseConnect
    {
        private readonly string _connectionString;
        private readonly MongoClient _client;
        private IMongoDatabase _database;
        private IMongoCollection<BsonDocument> _collection;
        
        public MongoConnect(string connectionString)
        {
            _connectionString = connectionString;
            _client = new MongoClient(_connectionString);
        }
        
        public void GetConnection(string DBname, string CollectionName)
        {
            _database = _client.GetDatabase(DBname);
            _collection = _database.GetCollection<BsonDocument>(CollectionName);
        }

        public IMongoCollection<BsonDocument> Collection
        {
            get => _collection;
            private set => _collection = value;
        }
    }
}
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Twitter
{
    class Program
    {
        static IMongoClient client = null;
        static IMongoDatabase database = null;

        static IMongoDatabase GetConnection()
        {
            if (database == null)
            {
                client = new MongoClient();
                database = client.GetDatabase("Twitter");
            }
            return database;
        }
        static void Main(string[] args)
        {
            Console.WriteLine("0 = How many Twitter users are in our database?");
            Console.WriteLine("1 = Which Twitter users link the most to other Twitter users?");
            Console.WriteLine("2 = Who is are the most mentioned Twitter users?");
            Console.WriteLine("3 = Who are the most active Twitter users?");
            Console.WriteLine("4 = Who are the five most grumpy?");
            Console.WriteLine("5 = Who are the five most positive?");
            while (true)
            {
                Console.WriteLine("Chose function to run:");
                int i = 10;
                bool result = int.TryParse(Console.ReadLine().ToString(), out i);
                Console.WriteLine("Wait for result..");
                switch (i)
                {
                    case 0:
                        Query1();
                        break;
                    case 1:
                        Query2();
                        break;
                    case 2:
                        Query3();
                        break;
                    case 3:
                        Query4();
                        break;
                    case 4:
                        Query5Negative();
                        break;
                    case 5:
                        Query5Positive();
                        break;
                    default:
                        break;
                }
            }
        }

        public static void Query1()
        {
            IMongoDatabase database = GetConnection();
            try
            {
                var tweets = database.GetCollection<BsonDocument>("Tweet");
                var filter = tweets.Distinct<dynamic>("user",new BsonDocument());
                var results = filter.ToList();  
                Console.WriteLine(results.Count.ToString());

            }
            catch (Exception e)
            {

                throw;
            }
        }
        public static void Query2()
        {
            IMongoDatabase database = GetConnection();
            try
            {
                var tweets = database.GetCollection<BsonDocument>("Tweet");
                var aggregate = tweets.Aggregate()
                    .Match(new BsonDocument { { "text", new Regex(@"@\w+") } })
                    .Group(new BsonDocument { { "_id", "$user" }, { "count", new BsonDocument("$sum", 1) } })
                    .Sort(new BsonDocument("count", -1))
                    .Limit(10);
                var results = aggregate.ToList();
                foreach (var item in results)
                {
                    Console.WriteLine(item.ToString());
                }

            }
            catch (Exception e)
            {

                throw;
            }
        }

        public static void Query3()
        {
            IMongoDatabase database = GetConnection();
            try
            {
                var options = new MapReduceOptions<BsonDocument,BsonDocument>();
                options.Filter = new BsonDocument { };
                options.Sort = new BsonDocument { { "value", 1 } };
                options.Limit = 5;
                var tweets = database.GetCollection<BsonDocument>("Tweet");
                var map = new BsonJavaScript(@"function(){var match=this.text.match(/"+new Regex(@"@\w+")+"/); var key=match?match[0]:null; if(key){emit(key,1);}}");
                var reduce = new BsonJavaScript(@"function(key,value){return Array.sum(value);}");
                var mapReduce = tweets.MapReduce(map,reduce, options).ToList();
                foreach (var item in mapReduce)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            catch (Exception e)
            {

                throw;
            }
        }
        public static void Query4()
        {
            IMongoDatabase database = GetConnection();
            try
            {
                var tweets = database.GetCollection<BsonDocument>("Tweet");
                var aggregate = tweets.Aggregate()
                    .Group(new BsonDocument { { "_id", "$user" }, { "count", new BsonDocument("$sum", 1) } })
                    .Sort(new BsonDocument("count", -1))
                    .Limit(10);
                var results = aggregate.ToList();
                foreach (var item in results)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            catch (Exception e)
            {

                throw;
            }
        }
        public static void Query5Negative()
        {
            IMongoDatabase database = GetConnection();
            try
            {
                var tweets = database.GetCollection<BsonDocument>("Tweet");
                var aggregate = tweets.Aggregate()
                    .Match(new BsonDocument { { "polarity", 0 } })
                    .Group(new BsonDocument { { "_id", "$user" }, { "count", new BsonDocument("$sum", 1) } })
                    .Sort(new BsonDocument("count", -1))
                    .Limit(10);
                var results = aggregate.ToList();
                foreach (var item in results)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            catch (Exception e)
            {

                throw;
            }
        }
        public static void Query5Positive()
        {
            IMongoDatabase database = GetConnection();
            try
            {
                var tweets = database.GetCollection<BsonDocument>("Tweet");
                var aggregate = tweets.Aggregate()
                    .Match(new BsonDocument { { "polarity", 4 } })
                    .Group(new BsonDocument { { "_id", "$user" }, { "count", new BsonDocument("$sum", 1) } })
                    .Sort(new BsonDocument("count", -1))
                    .Limit(10);
                var results = aggregate.ToList();
                foreach (var item in results)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            catch (Exception e)
            {

                throw;
            }
        }
    }
}

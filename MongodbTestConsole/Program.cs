using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongodbTestConsole
{
    class Program
    {
        static void Main(string[] args)
        {

            var connectionString = "mongodb://localhost";
            var count = 10000000;
            var mongoClient = new MongoClient(connectionString);
            var server = mongoClient.GetServer();
            var db = server.GetDatabase("playground");
            var coll = db.GetCollection("test_10M");
            var bulk = coll.InitializeOrderedBulkOperation();
            var stopwatch = new Stopwatch();
            /*
            var buffer = 100000;
            int i = 0;
            stopwatch.Start();
            while (buffer <= count)
            {
                if (i < buffer)
                {
                    bulk = coll.InitializeOrderedBulkOperation();
                    for (; i < buffer; i++)
                    {
                        var person = new Person() { Id = (i + 1), Name = "John_" + (i + 1).ToString(), Random = Guid.NewGuid().ToString() };
                        bulk.Insert<Person>(person);
                        person = null;
                    }
                    buffer = (buffer + 100000) < count ? (buffer + 100000) : count;
                    bulk.Execute();
                }
            }

            stopwatch.Stop();
            Console.WriteLine(stopwatch.Elapsed);
            Console.WriteLine("[x] Insert Done.");
             * */
            bulk = coll.InitializeUnorderedBulkOperation();
            stopwatch.Reset();
            stopwatch.Start();

            var query = Query.LT("Id", count + 1);
            var update = Update.Set("Random", Guid.NewGuid().ToString());
            //for (int i = 0; i < count; i++)
            //{

            //    bulk.Find (query ).Lim UpdateOne(Update.Set("Random", Guid.NewGuid().ToString())); 
            //}
            /*
            var sortBy = SortBy.Descending("Id");
            var persons = coll.FindAllAs<Person>().SetSortOrder(sortBy).SetLimit(1);
            var maxId = persons.First().Id;
             * */
            // persons.ToList().ForEach(p => p.Random = Guid.NewGuid);
            coll.Update(query, update, UpdateFlags.Multi);
            stopwatch.Stop();
            Console.WriteLine(stopwatch.Elapsed);
            Console.WriteLine("[x] Update Done.");
            Console.Read();
        }

        class Person
        {
            public string Name { get; set; }
            public long Id { get; set; }

            public string Random { get; set; }
        }
    }
}

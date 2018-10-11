using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Deployment;
using Apache.Ignite.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GettingStarted
{
    class Program
    {
        static void Main(string[] args)
        {
            //  Compute();
            //Datarid();
            //Ignition.Start();
            // DistributedCache();
            //ComplexObjects();
            //SCanQuery();
            //SQLQuery();
            //Join();
            LinqQuery();
            Console.ReadLine();
        }

        private static void ComplexObjects()
        {
            var cfg = new IgniteConfiguration
            {
                // Register custom class for Ignite serialization
                BinaryConfiguration = new BinaryConfiguration(typeof(Person))
            };
            IIgnite ignite = Ignition.Start(cfg);

            ICache<int, Person> cache = ignite.GetOrCreateCache<int, Person>("persons");
            cache[1] = new Person { Name = "John Doe", Age = 27 };

            foreach (ICacheEntry<int, Person> cacheEntry in cache)
                Console.WriteLine(cacheEntry);

            var binCache = cache.WithKeepBinary<int, IBinaryObject>();
            IBinaryObject binPerson = binCache[1];
            Console.WriteLine(binPerson.GetField<string>("Name"));
        }

        private static void Join()
        {
            Environment.SetEnvironmentVariable("IGNITE_H2_DEBUG_CONSOLE", "true");
            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
        typeof(Organization))
            };
            IIgnite ignite = Ignition.Start(cfg);

            ICache<int, Person> personCache = ignite.GetOrCreateCache<int, Person>(
                new CacheConfiguration("persons", typeof(Person)));

            var orgCache = ignite.GetOrCreateCache<int, Organization>(
                new CacheConfiguration("orgs", typeof(Organization)));

            personCache[1] = new Person { Name = "John Doe", Age = 27, OrgId = 1 };
            personCache[2] = new Person { Name = "Jane Moe", Age = 43, OrgId = 2 };
            personCache[3] = new Person { Name = "Ivan Petrov", Age = 59, OrgId = 2 };

            orgCache[1] = new Organization { Id = 1, Name = "Contoso" };
            orgCache[2] = new Organization { Id = 2, Name = "Apache" };

            var fieldsQuery = new SqlFieldsQuery(
                "select Person.Name from Person " +
                "join \"orgs\".Organization as org on (Person.OrgId = org.Id) " +
                "where org.Name = ?", "Apache");

            foreach (var fieldList in personCache.QueryFields(fieldsQuery))
                Console.WriteLine(fieldList[0]);  // Jane Moe, Ivan Petrov
        }

        private static void LinqQuery()
        {
            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
       typeof(Organization))
            };
            IIgnite ignite = Ignition.Start(cfg);

         
            ICache<int, Person> personCache = ignite.GetOrCreateCache<int, Person>(
                new CacheConfiguration("persons", typeof(Person)));

            var orgCache = ignite.GetOrCreateCache<int, Organization>(
                new CacheConfiguration("orgs", typeof(Organization)));

            personCache[1] = new Person { Name = "John Doe", Age = 27, OrgId = 1 };
            personCache[2] = new Person { Name = "Jane Moe", Age = 43, OrgId = 2 };
            personCache[3] = new Person { Name = "Ivan Petrov", Age = 59, OrgId = 2 };

            orgCache[1] = new Organization { Id = 1, Name = "Contoso" };
            orgCache[2] = new Organization { Id = 2, Name = "Apache" };
            IQueryable<ICacheEntry<int, Person>> persons = personCache.AsCacheQueryable();
            IQueryable<ICacheEntry<int, Organization>> orgs = orgCache.AsCacheQueryable();

            // Simple filtering
            IQueryable<ICacheEntry<int, Person>> qry = persons.Where(e => e.Value.Age > 30);

            // Fields query
            IQueryable<string> fieldsQry = persons
                .Where(e => e.Value.Age > 30)
                .Select(e => e.Value.Name);

            // Aggregate
            int sum = persons.Sum(e => e.Value.Age);

            // Join
            IQueryable<string> join = persons
                .Join(orgs.Where(org => org.Value.Name == "Apache"),
                    person => person.Value.OrgId,
                    org => org.Value.Id,
                    (person, org) => person.Value.Name);

            // Join with query syntax
            var join2 = from person in persons
                        from org in orgs
                        where person.Value.OrgId == org.Value.Id && org.Value.Name == "Apache"
                        select person.Value.Name;
        }

        private static void SCanQuery()
        {
            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
           typeof(PersonFilter))
            };
            IIgnite ignite = Ignition.Start(cfg);

            ICache<int, Person> cache = ignite.GetOrCreateCache<int, Person>("persons");
            cache[1] = new Person { Name = "John Doe", Age = 27 };
            cache[2] = new Person { Name = "Jane Moe", Age = 43 };

            var scanQuery = new ScanQuery<int, Person>(new PersonFilter());
            IQueryCursor<ICacheEntry<int, Person>> queryCursor = cache.Query(scanQuery);

            foreach (ICacheEntry<int, Person> cacheEntry in queryCursor)
                Console.WriteLine(cacheEntry);
        }

        private static void SQLQuery()
        {
            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
           typeof(PersonFilter))
            };
            IIgnite ignite = Ignition.Start(cfg);

            //  ICache<int, Person> cache = ignite.GetOrCreateCache<int, Person>("persons");
            ICache<int, Person> cache = ignite.GetOrCreateCache<int, Person>(
                                        new CacheConfiguration("persons", typeof(Person)));
            cache[1] = new Person { Name = "John Doe", Age = 27 };
            cache[2] = new Person { Name = "Jane Moe", Age = 43 };

            var sqlQuerry = new SqlQuery(typeof(Person), "where age > ?", 30);
            IQueryCursor<ICacheEntry<int, Person>> queryCursor = cache.Query(sqlQuerry);

            foreach (ICacheEntry<int, Person> cacheEntry in queryCursor)
                Console.WriteLine(cacheEntry);

            var fieldsQuery = new SqlFieldsQuery(     "select name from Person where age > ?", 30);
           var queryCursor2 = cache.Query(fieldsQuery);

            foreach (var fieldList in queryCursor2)
                Console.WriteLine(fieldList[0]);

            var fieldsQuery2 = new SqlFieldsQuery("select sum(age) from Person");
            var queryCursor3 = cache.Query(fieldsQuery2);
            Console.WriteLine(queryCursor3.GetAll()[0][0]);   // 70


        }


        private static void DistributedCache()
        {
            IIgnite ignite = Ignition.Start();
            ICache<int, string> cache = ignite.GetOrCreateCache<int, string>("test");
            if (cache.PutIfAbsent(1, "Hello, World!"))
                Console.WriteLine("Added a value to the cache!");
            else
                Console.WriteLine(cache.Get(1));
        }

        static void Datarid()
        {
            using (var ignite = Ignition.Start())
            {
                var cache = ignite.GetOrCreateCache<int, string>("myCache");

                // Store keys in cache (values will end up on different cache nodes).
                for (int i = 0; i < 10; i++)
                    cache.Put(i, i.ToString());

                for (int i = 0; i < 10; i++)
                    Console.WriteLine("Got [key={0}, val={1}]", i, cache.Get(i));
            }
        }

        static void Compute()
        {
            var cfg = new IgniteConfiguration
            {
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain
            };
            using (var ignite = Ignition.Start())
            {
                var funcs = "Count characters using callable".Split(' ')
                  .Select(word => new ComputeFunc { Word = word });

                ICollection<int> res = ignite.GetCompute().Call(funcs);

                var sum = res.Sum();

                Console.WriteLine(">>> Total number of characters in the phrase is '{0}'.", sum);

                ignite.GetCompute().Broadcast(new HelloAction());
            }
        }

        class PersonFilter : ICacheEntryFilter<int, Person>
        {
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return entry.Value.Age > 30;
            }
        }

        class Organization
        {
            [QuerySqlField]
            public string Name { get; set; }

            [QuerySqlField]
            public int Id { get; set; }
        }

        class Person
        {
            [QuerySqlField]
            public string Name { get; set; }
            [QuerySqlField]
            public int Age { get; set; }
            [QuerySqlField]
            public int OrgId { get; set; }
            public override string ToString()
            {
                return $"Person [Name={Name}, Age={Age}]";
            }
        }
        class ComputeFunc : IComputeFunc<int>
        {
            public string Word { get; set; }

            public int Invoke()
            {
                return Word.Length;
            }
        }

        class HelloAction : IComputeAction
        {
            public void Invoke()
            {
                Console.WriteLine("Hello, World!");
            }
        }

    }
}

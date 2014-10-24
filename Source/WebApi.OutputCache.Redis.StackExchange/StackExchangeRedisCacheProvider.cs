using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using WebApi.OutputCache.Core.Cache;

namespace WebApi.OutputCache.Redis.StackExchange
{
	public class StackExchangeRedisCacheProvider : IApiOutputCache
	{
		private ConnectionMultiplexer _connection;
		private IDatabase _database;

		public StackExchangeRedisCacheProvider(string connectionString)
		{
			Init(ConnectionMultiplexer.Connect(connectionString));
		}

		public StackExchangeRedisCacheProvider(ConnectionMultiplexer connection)
		{
			Init(connection);
		}

		public StackExchangeRedisCacheProvider(IDatabase database)
		{
			_database = database;
		}

		private void Init(ConnectionMultiplexer connection)
		{
			_connection = connection;
			_database = _connection.GetDatabase();
		}

		public void Dispose()
		{
			if (_connection != null)
				_connection.Dispose();
		}

		public void RemoveStartsWith(string key)
		{
			var keys = _database.SetMembers(key);

			foreach (var memberKey in keys)
			{
				Remove(memberKey);
			}

			Remove(key);
		}

		public T Get<T>(string key) where T : class
		{
			var result = _database.StringGet(key);

			return result as T;
		}

		public object Get(string key)
		{
			// Need to return this as a direct OBJECT and not the RedisValue because it can't convert using 'as string' or 'as byte[]' correctly :(
			dynamic result = _database.StringGet(key);
			return result;
		}

		public void Remove(string key)
		{
			var result = _database.KeyDelete(key);
		}

		public bool Contains(string key)
		{
			var exists = _database.KeyExists(key);
			return exists;
		}

		public void Add(string key, object o, DateTimeOffset expiration, string dependsOnKey = null)
		{
			// Lets not store the base type (will be dependsOnKey later) since we want to use it as a set!
			if (Equals(o, "")) return;

			// We'd rather just store it as an object type in the database and not have to convert it to RedisValue!
			RedisValue value;
			if (o is string)
			{
				value = o as string;
			}
			else if (o is byte[])
			{
				value = o as byte[];
			}
			else
			{
				// Need to try more casts first...
				value = o.ToString();
			}

			var primaryAdded = _database.StringSet(key, value, expiration.Subtract(DateTimeOffset.Now));
			if (dependsOnKey != null && primaryAdded)
			{
				_database.SetAdd(dependsOnKey, key);
			}
		}

		public IEnumerable<string> AllKeys { get; private set; }
	}
}

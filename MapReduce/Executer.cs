using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace MapReduce
{
	public class Executer<TMapInput, TReduceInput>
	{
		private readonly int batchSize;
		private readonly MapReduceTask<TMapInput, TReduceInput> task;

		public Executer(int batchSize, MapReduceTask<TMapInput, TReduceInput> task)
		{
			this.batchSize = batchSize;
			this.task = task;
		}

		public void Execute(IEnumerable<TMapInput> raw)
		{
			ExecuteMap(raw);

			var keysToProcess = Storage.GetKeysToProcess().ToArray();
			for (int i = 0; i < 3; i++)
			{
				foreach (var key in keysToProcess)
				{
					ExecuteReduce(key, i + 1);
				}
			}

		}

		private void ExecuteReduce(string key, int level)
		{
			var hasMore = new Ref<bool> { Value = true };

			while (hasMore.Value)
			{
				var persistedResults = Storage.GetUnprocessedFor(key, level, batchSize, hasMore).ToArray();
				if (persistedResults.Length == 0)
				{
					Storage.Delete(key);
					break;
				}

				Storage.MarkProcessed(key, level, persistedResults);

				foreach (var items in persistedResults.GroupBy(x => x.BucketId / batchSize).ToArray())
				{
					var results = task.Reduce(items.SelectMany(x => x.Values)).ToArray();
					Storage.PersistReduce(key, level + 1, items.Key, results);
					if (level < 3 )
						continue;
					foreach (var reduceInput in results)
					{
						Console.WriteLine(reduceInput);
					}
				}
			}
		}

		private void ExecuteMap(IEnumerable<TMapInput> raw)
		{
			foreach (var partition in raw.Partition(batchSize))
			{
				foreach (var reduceInput in from tuple in task.Map(partition) group tuple by new { Id = tuple.Item1, Reduce = task.GetReduceKey(tuple.Item2) })
				{
					var absStableInvariantIgnoreCaseStringHash = AbsStableInvariantIgnoreCaseStringHash(reduceInput.Key.Id.ToString());
					var bucket = absStableInvariantIgnoreCaseStringHash % (batchSize * batchSize);
					Storage.PersistMap(reduceInput.Key.Reduce, reduceInput.Key.Id, bucket, reduceInput.Select(x => x.Item2));
				}
			}
		}

		private int AbsStableInvariantIgnoreCaseStringHash(string s)
		{
			return Math.Abs(s.Aggregate(0, (current, ch) => (char.ToUpperInvariant(ch).GetHashCode() * 397) ^ current));
		}

		public static class Storage
		{
			public static void PersistMap(string reduceKey, Guid id, int bucket, IEnumerable<TReduceInput> values)
			{
				var dir = Path.Combine("MapResults", "Pending", reduceKey);
				if (Directory.Exists(dir) == false)
					Directory.CreateDirectory(dir);
				var serializeObject = JsonConvert.SerializeObject(new PersistedResult<TReduceInput>
					{
						BucketId = bucket,
						Id = id,
						Key = reduceKey,
						Values = values
					}, Formatting.Indented);
				File.WriteAllText(Path.Combine(dir, id + ".json"), serializeObject);
			}

			public static IEnumerable<string> GetKeysToProcess()
			{
				var path = Path.Combine("MapResults", "Pending");
				return Directory.GetDirectories(path).Select(x => x.Substring(path.Length + 1));
			}

			public static IEnumerable<PersistedResult<TReduceInput>> GetUnprocessedFor(string key, int level, int size, Ref<bool> hasMore)
			{
				hasMore.Value = false;
				int lastBucketId = -1;
				IEnumerable<PersistedResult<TReduceInput>> results;
				switch (level)
				{
					case 1:
						results = GetPersistedMapResults(key);
						break;
					case 2:
					case 3:
						results = GetPersistedReduceResults(key, level);
						break;
					default:
						throw new InvalidOperationException("Unknown level " + level);
				}
				foreach (var persistedResult in results.OrderBy(x => x.BucketId))
				{
					size -= 1;
					if (size < 0 && lastBucketId != persistedResult.BucketId)
					{
						hasMore.Value = true;
						yield break;
					}
					lastBucketId = persistedResult.BucketId;
					yield return persistedResult;
				}
			}

			private static IEnumerable<PersistedResult<TReduceInput>> GetPersistedReduceResults(string key, int level)
			{
				var dir = Path.Combine("ReduceResults", "Level-" + level, "Pending", key);
				foreach (var file in Directory.GetFiles(dir, "*.json"))
				{
					var readAllText = File.ReadAllText(file);
					var deserializeObject = JsonConvert.DeserializeObject<PersistedResult<TReduceInput>>(readAllText);
					deserializeObject.File = file;
					yield return deserializeObject;
				}
			}

			private static IEnumerable<PersistedResult<TReduceInput>> GetPersistedMapResults(string key)
			{
				var path = Path.Combine("MapResults", "Pending", key);
				foreach (var file in Directory.GetFiles(path, "*.json"))
				{
					var readAllText = File.ReadAllText(file);
					var deserializeObject = JsonConvert.DeserializeObject<PersistedResult<TReduceInput>>(readAllText);
					deserializeObject.File = file;
					yield return deserializeObject;
				}
			}

			public static int GetNextBucketIdFor(string key, string purpose)
			{
				var dir = Path.Combine("BucketIds", purpose);
				if (Directory.Exists(dir) == false)
					Directory.CreateDirectory(dir);
				var path = Path.Combine(dir, key + ".json");
				if (File.Exists(path) == false)
				{
					File.WriteAllText(path, "1");
					return 1;
				}
				var i = int.Parse(File.ReadAllText(path));
				File.WriteAllText(path, (i + 1).ToString(CultureInfo.InvariantCulture));
				return i + 1;
			}

			public static void UpdateBucketId(string key, int level, IEnumerable<PersistedResult<TReduceInput>> persistedResults, int bucketId)
			{
				string path;
				if (level == 1)
				{
					path = Path.Combine("MapResults", key, "Buckets", bucketId.ToString(CultureInfo.InvariantCulture));
				}
				else
				{
					path = Path.Combine("ReduceResults", "Level-" + level, "Buckets", key, bucketId.ToString(CultureInfo.InvariantCulture));
				}

				if (Directory.Exists(path) == false)
					Directory.CreateDirectory(path);

				foreach (var persistedResult in persistedResults)
				{
					File.Move(persistedResult.File, Path.Combine(path, Path.GetFileName(persistedResult.File)));
				}
			}

			public static void PersistReduce(string key, int level, int bucketId, IEnumerable<TReduceInput> values)
			{
				var dir = Path.Combine("ReduceResults", "Level-" + level, "Pending", key);
				if (Directory.Exists(dir) == false)
					Directory.CreateDirectory(dir);
				var serializeObject = JsonConvert.SerializeObject(new PersistedResult<TReduceInput>
				{
					BucketId = bucketId,
					Key = key,
					Values = values,
					Level = level
				}, Formatting.Indented);
				File.WriteAllText(Path.Combine(dir, Path.GetFileName(Path.GetTempFileName()) + ".json"), serializeObject);

			}

			public static void MarkProcessed(string key, int level, PersistedResult<TReduceInput>[] persistedResults)
			{
				foreach (var items in persistedResults.GroupBy(x => x.BucketId))
				{
					UpdateBucketId(key, level, items, items.Key);
				}
			}

			public static void Delete(string key)
			{
				// currently not implemented ,need to for the real one
			}
		}

		public PersistedResult<TReduceInput> Query(string key)
		{
			return null;
		}
	}
}
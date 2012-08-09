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
		private const int batchSize = 1024;
		private readonly MapReduceTask<TMapInput, TReduceInput> task;

		public Executer(MapReduceTask<TMapInput, TReduceInput> task)
		{
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
					ExecuteReduce(key, i);
				}
			}

		}

		private void ExecuteReduce(string key, int level)
		{
			IEnumerable<PersistedResult<TReduceInput>> persistedResults;
			switch (level)
			{
				case 0:
					persistedResults = Storage.GetScheduledMapBucketsFor(key);
					break;
				case 1:
				case 2:
					persistedResults = Storage.GetScheduledReduceBucketsFor(key, level);
					break;
				default:
					throw new ArgumentException("Invalid level: " + level);
			}

			foreach (var items in persistedResults.GroupBy(x => x.BucketId/batchSize).ToArray())
			{
				var results = task.Reduce(items.SelectMany(x => x.Values)).ToArray();
				if (level < 2)
					Storage.PersistReduce(key, level + 1, items.Key, results);
				else
					Storage.PersistResult(key, results);
			}
		}

		private void ExecuteMap(IEnumerable<TMapInput> raw)
		{
			foreach (var partition in raw.Partition(batchSize))
			{
				var items = partition.ToArray();
				foreach (var mapInput in items)
				{
					var documentId = task.GetDocumentId(mapInput);
					Storage.DeleteDocumentIdResultsAndScheduleTheirBucketsForReduce(documentId);
				}

				foreach (var reduceInput in from tuple in task.Map(items) group tuple by new { Id = tuple.Item1, Reduce = task.GetReduceKey(tuple.Item2) })
				{
					var bucket = Storage.GetBucket(reduceInput.Key.Id);
					Storage.ScheduleReduction(reduceInput.Key.Reduce, bucket);
					Storage.PersistMap(reduceInput.Key.Reduce, reduceInput.Key.Id, bucket, reduceInput.Select(x => x.Item2));
				}
			}
		}


		public static class Storage
		{
			public static void ScheduleReduction(string key, int bucket)
			{
				foreach (var path in new[]
					{
						Path.Combine("Schedules", "Maps", key, bucket.ToString(CultureInfo.InvariantCulture)),
						Path.Combine("Schedules", "Reduce", "One", key, (bucket/ batchSize).ToString(CultureInfo.InvariantCulture)),
						Path.Combine("Schedules", "Reduce", "Two", key, ((bucket/ batchSize) / batchSize).ToString(CultureInfo.InvariantCulture)),
					})
				{
					var dir = Path.GetDirectoryName(path);
					if (Directory.Exists(dir) == false)
						Directory.CreateDirectory(dir);
					File.WriteAllText(path, "work");
				}

				// reset buckets
				foreach (var path in new[]
					{
						Path.Combine("ReduceResults", "One", key, (bucket/ batchSize).ToString(CultureInfo.InvariantCulture)),
						Path.Combine("ReduceResults", "Two", key, ((bucket/ batchSize) / batchSize).ToString(CultureInfo.InvariantCulture)),
					})
				{
					if (Directory.Exists(path))
						Directory.Delete(path, true);
				}

				var resultFile = Path.Combine("FinalResults", key);
				if(File.Exists(resultFile))
					File.Delete(resultFile);
			}

			public static IEnumerable<string> GetKeysToProcess()
			{
				//read from the schedule
				var path = Path.Combine("Schedules", "Maps");
				if (Directory.Exists(path) == false)
					yield break;
				foreach (var file in Directory.GetDirectories(path))
				{
					yield return Path.GetFileNameWithoutExtension(file);
				}
			}

			public static void PersistMap(string reduceKey, string id, int bucket, IEnumerable<TReduceInput> values)
			{
				WriteResults(id, Path.Combine("MapResults", reduceKey, bucket.ToString(CultureInfo.InvariantCulture)),
							 new PersistedResult<TReduceInput>
								 {
									 BucketId = bucket,
									 Id = id,
									 Key = reduceKey,
									 Values = values
								 });
			}

			private static void WriteResults(string id, string dir, PersistedResult<TReduceInput> persistedResult)
			{
				if (Directory.Exists(dir) == false)
					Directory.CreateDirectory(dir);
				var serializeObject = JsonConvert.SerializeObject(persistedResult, Formatting.Indented);
				File.WriteAllText(Path.Combine(dir, id), serializeObject);
			}

			private static void WriteResults(string dir, PersistedResult<TReduceInput> persistedResult)
			{
				WriteResults(Path.GetFileNameWithoutExtension(Path.GetTempFileName()), dir, persistedResult);
			}


			public static void PersistReduce(string key, int level, int bucketId, IEnumerable<TReduceInput> values)
			{
				var dir = Path.Combine("ReduceResults", GetLevelString(level), key, bucketId.ToString(CultureInfo.InvariantCulture));
				WriteResults(dir, new PersistedResult<TReduceInput>
					{
						BucketId = bucketId,
						Key = key,
						Values = values,
						Level = level
					});

			}

			private static string GetLevelString(int level)
			{
				string levelString;
				switch (level)
				{
					case 1:
						levelString = "One";
						break;
					case 2:
						levelString = "Two";
						break;
					default:
						throw new ArgumentException("Invalid level: " + level);
				}
				return levelString;
			}

			public static void Delete(string key)
			{
				var paths = new[]
					{
						Path.Combine("MapResults", key),
						Path.Combine("ReduceResults", "One", key),
						Path.Combine("ReduceResults", "Two", key),
					};

				foreach (var path in paths.Where(Directory.Exists))
				{
					Directory.Delete(path, true);
				}
				var result = Path.Combine("FinalResults", key);
				if (File.Exists(result))
					File.Delete(result);
			}

			public static void DeleteDocumentIdResultsAndScheduleTheirBucketsForReduce(string documentId)
			{
				if (Directory.Exists("MapResults") == false)
					return;

				foreach (var file in Directory.GetFiles("MapResults", documentId, SearchOption.AllDirectories))
				{
					var persistedResult = ReadResult(file);
					ScheduleReduction(persistedResult.Key, GetBucket(documentId));

					File.Delete(file);
				}
			}

			public static void PersistResult(string key, IEnumerable<TReduceInput> results)
			{
				var dir = "FinalResults";
				if (Directory.Exists(dir) == false)
					Directory.CreateDirectory(dir);
				var serializeObject = JsonConvert.SerializeObject(new PersistedResult<TReduceInput>
				{
					BucketId = -1,
					Key = key,
					Values = results,
					Level = 3
				}, Formatting.Indented);
				File.WriteAllText(Path.Combine(dir, key), serializeObject);
			}

			public static PersistedResult<TReduceInput> ReadResult(string key)
			{
				var file = Path.Combine("FinalResults", key);
				return ReadFromFile(file);
			}

			private static PersistedResult<TReduceInput> ReadFromFile(string file)
			{
				if (File.Exists(file) == false)
					return null;
				var readAllText = File.ReadAllText(file);
				var deserializeObject = JsonConvert.DeserializeObject<PersistedResult<TReduceInput>>(readAllText);
				deserializeObject.File = file;
				return deserializeObject;
			}


			public static int GetBucket(string id)
			{
				return AbsStableInvariantIgnoreCaseStringHash(id) % (batchSize * batchSize);
			}

			private static int AbsStableInvariantIgnoreCaseStringHash(string s)
			{
				return Math.Abs(s.Aggregate(0, (current, ch) => (char.ToUpperInvariant(ch).GetHashCode() * 397) ^ current));
			}

			public static IEnumerable<PersistedResult<TReduceInput>> GetScheduledMapBucketsFor(string key)
			{
				var path = Path.Combine("Schedules", "Maps", key);
				if(Directory.Exists(path) == false)
					yield break;

				foreach (var bucket in Directory.GetFiles(path))
				{
					var bucketPath = Path.Combine("MapResults", key, Path.GetFileNameWithoutExtension(bucket));
					if(Directory.Exists(bucketPath) == false)
						continue;

					foreach (var file in Directory.GetFiles(bucketPath))
					{
						yield return ReadFromFile(file);
					}
				}
			}

			public static IEnumerable<PersistedResult<TReduceInput>> GetScheduledReduceBucketsFor(string key, int level)
			{
				var levelString = GetLevelString(level);
				var path = Path.Combine("Schedules", "Reduce", levelString, key);
				if (Directory.Exists(path) == false)
					yield break;

				foreach (var bucket in Directory.GetFiles(path))
				{
					var bucketPath = Path.Combine("ReduceResults", levelString, key, Path.GetFileNameWithoutExtension(bucket));
					if (Directory.Exists(bucketPath) == false)
						continue;

					foreach (var file in Directory.GetFiles(bucketPath))
					{
						yield return ReadFromFile(file);
					}
				}
			}
		}

		public IEnumerable<TReduceInput> Query(string key)
		{
			var persistedResult = Storage.ReadResult(key);
			if (persistedResult == null)
				return Enumerable.Empty<TReduceInput>();
			return persistedResult.Values;
		}
	}
}
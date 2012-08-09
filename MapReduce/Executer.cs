using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using NLog;
using Newtonsoft.Json;

namespace MapReduce
{
	public class Executer
	{
		public static Executer<TMapInput, TReduceInput> Create<TMapInput, TReduceInput>(MapReduceTask<TMapInput, TReduceInput> task)
		{
			return new Executer<TMapInput, TReduceInput>(task);
		}
	}

	public class Executer<TMapInput, TReduceInput>
	{
		private static Logger logger = LogManager.GetCurrentClassLogger();

		private const int BatchSize = 1024;
		private readonly MapReduceTask<TMapInput, TReduceInput> task;

		public Executer(MapReduceTask<TMapInput, TReduceInput> task)
		{
			this.task = task;
		}

		public void Execute(IEnumerable<TMapInput> raw)
		{
			ExecuteMap(raw);

			var keysToProcess = Storage.GetKeysToProcess().ToArray();
			logger.Debug(()=>string.Format("Processing the following keys: [{0}]",string.Join(", ", keysToProcess)));
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

			var groupings = persistedResults.GroupBy(x => x.BucketId/BatchSize).ToArray();
			foreach (var items in groupings)
			{
				logger.Debug("Executing reduce for {0} level {1} with {2} batches for batch {3}", key, level, groupings.Count(), items.Key);
				var reduceInputs = items.SelectMany(x => x.Values).ToArray();
				var results = task.Reduce(reduceInputs).ToArray();

				if(logger.IsDebugEnabled)
				{
					logger.Debug("Reduce for key {0} level {3} - ({4} items) [{1}] to [{2}]",
						key,
						string.Join(", ", reduceInputs.Select(x=>x.ToString())),
						string.Join(", ", results.Select(x => x.ToString())),
						level,
						reduceInputs.Length
					);
				}

				if (level < 2)
					Storage.PersistReduce(key, level + 1, items.Key, results);
				else
					Storage.PersistResult(key, results);
			}
		}

		private void ExecuteMap(IEnumerable<TMapInput> raw)
		{
			foreach (var partition in raw.Partition(BatchSize))
			{
				var items = partition.ToArray();
				foreach (var mapInput in items)
				{
					var documentId = task.GetDocumentId(mapInput);
					logger.Debug("Removing old results for document: {0}", documentId);
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
						Path.Combine("Schedules", "Reduce", "One", key, (bucket/ BatchSize).ToString(CultureInfo.InvariantCulture)),
						Path.Combine("Schedules", "Reduce", "Two", key, ((bucket/ BatchSize) / BatchSize).ToString(CultureInfo.InvariantCulture)),
					})
				{
					var dir = Path.GetDirectoryName(path);
					if (Directory.Exists(dir) == false)
						Directory.CreateDirectory(dir);
					File.WriteAllText(path, "work");
					logger.Debug("Scheduling {0}", path);
				}

				// reset buckets
				foreach (var path in new[]
					{
						Path.Combine("ReduceResults", "One", key, (bucket/ BatchSize).ToString(CultureInfo.InvariantCulture)),
						Path.Combine("ReduceResults", "Two", key, ((bucket/ BatchSize) / BatchSize).ToString(CultureInfo.InvariantCulture)),
					})
				{
					if (Directory.Exists(path))
					{
						Directory.Delete(path, true);
						logger.Debug("Removing old reduce results for {0}", path);
					}
				}

				var resultFile = Path.Combine("FinalResults", key);
				if(File.Exists(resultFile))
				{
					File.Delete(resultFile);
					logger.Debug("Removing old final result for {0}", resultFile);
				}
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

			private static int counter;
			private static void WriteResults(string dir, PersistedResult<TReduceInput> persistedResult)
			{
				WriteResults((++counter).ToString(), dir, persistedResult);
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
					var persistedResult = ReadFromFile(file);
					logger.Debug("Deleting old result for {0} with reduce key {1}",
					             documentId, persistedResult.Key);
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
				return AbsStableInvariantIgnoreCaseStringHash(id) % (BatchSize * BatchSize);
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

				var nextBuckets = new HashSet<int>();
				foreach (var bucketString in Directory.GetFiles(path))
				{
					File.Delete(bucketString);
					var nextBucket = int.Parse(Path.GetFileNameWithoutExtension(bucketString)) / BatchSize;
					nextBuckets.Add(nextBucket);
				}

				var resultsPath = Path.Combine("MapResults", key);
				if (Directory.Exists(resultsPath) == false)
					yield break;

				foreach (var bucketString in Directory.GetDirectories(resultsPath))
				{
					var nextBucket = int.Parse(Path.GetFileNameWithoutExtension(bucketString)) / BatchSize;
					if (nextBuckets.Contains(nextBucket) == false)
						continue;

					foreach (var file in Directory.GetFiles(bucketString))
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

				var nextBuckets = new HashSet<int>();
				foreach (var bucketString in Directory.GetFiles(path))
				{
					File.Delete(bucketString);
					var nextBucket = int.Parse(Path.GetFileNameWithoutExtension(bucketString)) / BatchSize;
					nextBuckets.Add(nextBucket);
				}

				var resultsPath = Path.Combine("ReduceResults", levelString, key);
				if (Directory.Exists(resultsPath) == false)
					yield break;

				foreach (var bucketString in Directory.GetDirectories(resultsPath))
				{
					var nextBucket = int.Parse(Path.GetFileNameWithoutExtension(bucketString)) / BatchSize;
					if(nextBuckets.Contains(nextBucket) == false)
						continue;

					foreach (var file in Directory.GetFiles(bucketString))
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
using System;
using System.Collections.Generic;

namespace MapReduce
{
	public class PersistedResult<TReduceInput>
	{
		public string Key { get; set; }
		public Guid Id { get; set; }
		public IEnumerable<TReduceInput> Values { get; set; }
		public int BucketId { get; set; }

		public string File { get; set; }

		public int Level { get; set; }
	}
}
using System;
using System.Collections.Generic;

namespace MapReduce
{
	public abstract class MapReduceTask<TMapInput,TReduceInput>
	{
		public abstract IEnumerable<Tuple<string, TReduceInput>> Map(IEnumerable<TMapInput> items);

		public abstract IEnumerable<TReduceInput> Reduce(IEnumerable<TReduceInput> items);

		public abstract string GetReduceKey(TReduceInput input);

		public abstract string GetDocumentId(TMapInput input);
	}
}
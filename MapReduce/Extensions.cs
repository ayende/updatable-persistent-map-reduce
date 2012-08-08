using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
	public static class Extensions
	{
		public static IEnumerable<IEnumerable<T>> Partition<T>(this IEnumerable<T> self, int size)
		{
			var source = self.ToList();
			for (int i = 0; i < source.Count; i += size)
			{
				yield return source.Skip(i).Take(size).ToList();
			}
		}
	}
}
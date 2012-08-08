using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MapReduce
{
	class Program
	{
		static void Main(string[] args)
		{
			foreach (var directory in Directory.GetDirectories("."))
			{
				Directory.Delete(directory, true);
			}

			var people = PeopleFrom("CA", 379)
				.Concat(PeopleFrom("TX", 256));

			var executer = new Executer<Person, StatePopulation>(16, new PeopleCountByState());
			executer.Execute(people);
		}

		

		public static IEnumerable<Person> PeopleFrom(string state, int count)
		{
			for (int i = 0; i < count; i++)
			{
				yield return new Person
					{
						Id = Guid.NewGuid(),
						State = state
					};
			}
		}
	}

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

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

			var executer = new Executer<Person, StatePopulation>(1024, new PeopleCountByState());
			executer.Execute(people);

			var results = executer.Query("CA");
			foreach (var population in results)
			{
				Console.WriteLine(population);
			}
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
}

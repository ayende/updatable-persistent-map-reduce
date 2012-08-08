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
		static void Main()
		{
			foreach (var directory in Directory.GetDirectories("."))
			{
				Directory.Delete(directory, true);
			}

			var people = PeopleFrom("CA", 379)
				.Concat(PeopleFrom("TX", 256));

			var executer = new Executer<Person, StatePopulation>(1024, new PeopleCountByState());
			executer.Execute(people);

			PrintOutput(executer);
		}

		private static void PrintOutput(Executer<Person, StatePopulation> executer)
		{
			var results = executer.Query("CA");
			foreach (var population in results)
			{
				Console.WriteLine(population);
			}

			results = executer.Query("TX");
			foreach (var population in results)
			{
				Console.WriteLine(population);
			}
		}

		private static int _peopleCounter;

		public static IEnumerable<Person> PeopleFrom(string state, int count)
		{
			for (int i = 0; i < count; i++)
			{
				yield return new Person
					{
						Id = "people-" + (++_peopleCounter),
						State = state
					};
			}
		}
	}
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;

namespace MapReduce
{
	class Program
	{
		static void Main()
		{
			foreach (var directory in Directory.GetDirectories("."))
			{
				try
				{
					Directory.Delete(directory, true);
				}
				catch (Exception)
				{
				}
			}

			var people = PeopleFrom("CA", 3970)
				.Concat(PeopleFrom("TX", 2561))
				.ToArray();

			var executer = Executer.Create(new PeopleCountByState());
			executer.Execute(people);
			Console.Clear();
			var val = PrintOutput(executer);

			executer.Execute(new[] {new Person
				{
					Id = "people-300",
					State = "CA"
				}, });

			var next = PrintOutput(executer);

			if (val != next)
				return;
		}

		private static int PrintOutput(Executer<Person, StatePopulation> executer)
		{
			var value = executer.Query("CA").Concat(executer.Query("TX")).Sum(x => x.Count);

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

			Console.WriteLine(value);
			return value;

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

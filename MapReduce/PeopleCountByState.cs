using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
	public class PeopleCountByState : MapReduceTask<Person, StatePopulation>
	{
		public override IEnumerable<Tuple<Guid, StatePopulation>> Map(IEnumerable<Person> people)
		{
			return from person in people
			       select Tuple.Create(person.Id, new StatePopulation
				       {
					       Count = 1,
					       State = person.State
				       });

		}

		public override IEnumerable<StatePopulation> Reduce(IEnumerable<StatePopulation> items)
		{
			return from item in items
			       group item by item.State
			       into g
			       select new StatePopulation
				       {
					       Count = g.Sum(x => x.Count),
					       State = g.Key
				       };

		}

		public override string GetReduceKey(StatePopulation input)
		{
			return input.State;
		}
	}
}
using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
	public abstract class MapReduceTask<TMapInput,TReduceInput>
	{
		public abstract IEnumerable<Tuple<Guid, TReduceInput>> Map(IEnumerable<TMapInput> items);

		public abstract IEnumerable<TReduceInput> Reduce(IEnumerable<TReduceInput> items);

		public abstract string GetReduceKey(TReduceInput input);
	}

	public class Person
	{
		public string State { get; set; }

		public Guid Id { get; set; }
	}

	public class StatePopulation
	{
		public string State { get; set; }
		public int Count { get; set; }

		public override string ToString()
		{
			return string.Format("State: {0}, Count: {1}", State, Count);
		}

		protected bool Equals(StatePopulation other)
		{
			return string.Equals(State, other.State) && Count == other.Count;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((StatePopulation) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((State != null ? State.GetHashCode() : 0)*397) ^ Count;
			}
		}
	}

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
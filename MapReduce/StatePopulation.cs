namespace MapReduce
{
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
}
using System.Runtime.Serialization;

namespace CsharpSolution;

public class BruhMoment : Exception
{
	public BruhMoment()
	{
	}

	public BruhMoment(string? message) : base(message)
	{
	}

	public BruhMoment(string? message, Exception? innerException) : base(message, innerException)
	{
	}
}

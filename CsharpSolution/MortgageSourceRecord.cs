namespace CsharpSolution;

public struct MortgageSourceRecord
{
	public string Id {get; init;}
	public float Notional {get; init;}
	public float Interest_Rate {get; init;}
	public int Reset_Frequency {get; init;}
	public DateTime Start_Date {get; init;}
	public int Term {get; init;}
	public float Remaining_Notional {get; init;}
	public string Payment_Type {get; init;}
	public int Risk_Indicator {get; init;}
	public DateTime Next_Reset_Date {get; init;}
}

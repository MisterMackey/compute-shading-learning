namespace CsharpSolution;

public struct MortgageOutputRecord
{
	public string Id {get; init;}
	public float Interest_Rate {get; init;}
	public int Reset_Frequency {get; init;}
	public float Remaining_Notional {get; init;}
	public int Risk_Indicator {get; init;}
	public DateTime Next_Reset_Date {get; init;}
	public DateTime Date_Of_Payment {get; init;}
	public float monthly_payment {get; init;}
	public float Repayment_Payment {get; init;}
	public float Interest_Payment {get; init;}
	public float WriteOff {get; init;}
}

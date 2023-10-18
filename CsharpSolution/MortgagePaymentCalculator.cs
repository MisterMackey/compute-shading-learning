using System.Collections.Concurrent;
using System.ComponentModel;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;

namespace CsharpSolution;

public class MortgagePaymentCalculator
{
	private readonly DateTime ASSUMED_START_DATE = new DateTime(2023, 1, 1);

	private readonly Dictionary<int, (float up, float down)> RiskMigration = new Dictionary<int, (float down, float up)>
	{
		{0, (0.0f, 1 - 0.001f)},
		{1, (0.1f, 1 - 0.01f)},
		{2, (0.05f, 1 - 0.01f)},
		{3, (0.05f, 1 - 0.05f)},
		{4, (0.2f, 1 - 0.1f)},
	};

	private readonly Dictionary<int, float> DurationSpread = new Dictionary<int, float>
	{
		{30, 2.2f},
		{25, 1.9f},
		{20, 1.5f},
		{15, 1.0f},
		{10, 0.5f},
		{9, 0.4f},
		{7, 0.1f},
		{5, 0.0f},
	};

	private readonly Dictionary<int, float> RiskSpread = new Dictionary<int, float>
	{
		{0, 0.0f},
		{1, 0.3f},
		{2, 1.1f},
		{3, 1.9f},
		{4, 3.5f},
		{5, 100.0f},
	};
	
	private readonly ParquetSchema outputSchema = new ParquetSchema(
		new DataField<string>("Id"),
		new DataField<float>("Interest_Rate"),
		new DataField<int>("Reset_Frequency"),
		new DataField<float>("Remaining_Notional"),
		new DataField<int>("Risk_Indicator"),
		new DataField<DateTime>("Next_Reset_Date"),
		new DataField<DateTime>("Date_Of_Payment"),
		new DataField<float>("monthly_payment"),
		new DataField<float>("Repayment_Payment"),
		new DataField<float>("Interest_Payment"),
		new DataField<float>("WriteOff")
	);

	private readonly ILogger _logger;

	public MortgagePaymentCalculator(ILogger<MortgagePaymentCalculator> logger)
	{
		_logger = logger;
	}

	public async Task Run()
	{
		_logger.LogInformation("Starting Mortgage Payment Calculator");
		_logger.LogInformation("Loading data");
		(DataColumn[] columns, ParquetSchema schema) = await LoadData();
		_logger.LogInformation("Data loaded");
		_logger.LogInformation("Transforming to table");
		var table = TransformToTable(columns);
		_logger.LogInformation("Data transformed to row based");
		_logger.LogInformation("Computing mortgage payments");
		var result = ComputeAllMortgagePayments(table, ASSUMED_START_DATE);
		_logger.LogInformation("Mortgage payments computed, count = {0}", result.Length);
		_logger.LogInformation("Writing results to outcome.parquet");
		await WriteResults(result);
		_logger.LogInformation("Results written");
		_logger.LogInformation("Mortgage Payment Calculator finished");
	}

	private async Task WriteResults(MortgageOutputRecord[] result)
	{
		using Stream fileStream = new FileStream("outcome.parquet", FileMode.Create);
		using ParquetWriter writer = await ParquetWriter.CreateAsync(outputSchema, fileStream);
		writer.CompressionMethod = CompressionMethod.Snappy;
		writer.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;
		using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[0], result.Select(x => x.Id).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[1], result.Select(x => x.Interest_Rate).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[2], result.Select(x => x.Reset_Frequency).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[3], result.Select(x => x.Remaining_Notional).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[4], result.Select(x => x.Risk_Indicator).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[5], result.Select(x => x.Next_Reset_Date).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[6], result.Select(x => x.Date_Of_Payment).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[7], result.Select(x => x.monthly_payment).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[8], result.Select(x => x.Repayment_Payment).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[9], result.Select(x => x.Interest_Payment).ToArray()));
		await groupWriter.WriteColumnAsync(new DataColumn(outputSchema.DataFields[10], result.Select(x => x.WriteOff).ToArray()));
	}

	private async Task<(DataColumn[], ParquetSchema)> LoadData()
	{
		using ParquetReader reader = await ParquetReader.CreateAsync("../test_data.parquet");
		var schema = reader.Schema;
		using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0);
		DataColumn[] columns = new DataColumn[schema.Fields.Count];
		int i =0;
		foreach (var field in schema.DataFields)
		{
			columns[i++] = await groupReader.ReadColumnAsync(field);
		}
		_logger.LogInformation("Finished loading {0} rows of data", groupReader.RowCount);
		return (columns, schema);
	}

	private MortgageSourceRecord[] TransformToTable(DataColumn[] columns)
	{
		int count = columns[0].Data.Length;
		var table = Enumerable.Range(0, count).Select(i => new MortgageSourceRecord{Id=columns[0].Data.GetValue(i)!.ToString()!,
				Notional=float.Parse(columns[1].Data.GetValue(i)!.ToString()!),
				Interest_Rate=float.Parse(columns[2].Data.GetValue(i)!.ToString()!),
				Reset_Frequency=int.Parse(columns[3].Data.GetValue(i)!.ToString()!),
				Start_Date=DateTime.Parse(columns[4].Data.GetValue(i)!.ToString()!),
				Term=int.Parse(columns[5].Data.GetValue(i)!.ToString()!),
				Remaining_Notional=float.Parse(columns[6].Data.GetValue(i)!.ToString()!),
				Payment_Type=columns[7].Data.GetValue(i)!.ToString()!,
				Risk_Indicator=int.Parse(columns[8].Data.GetValue(i)!.ToString()!),
				Next_Reset_Date=DateTime.Parse(columns[9].Data.GetValue(i)!.ToString()!)
			}).ToArray();
		_logger.LogInformation("Transformed {0} rows of data", count);
		return table;
	}

	private MortgageOutputRecord[] ComputeAllMortgagePayments(MortgageSourceRecord[] input, DateTime assumedStartDate)
	{
		var bag = new ConcurrentBag<MortgageOutputRecord[]>();
		Parallel.ForEach(input, new ParallelOptions{MaxDegreeOfParallelism=1}
			,(record) => {
			var output = ComputeMortgagePaymentsForOneContract(record, assumedStartDate);
			bag.Add(output);
		});
		var output = bag.SelectMany(x => x).ToArray();
		return output;
	}

	private MortgageOutputRecord[] ComputeMortgagePaymentsForOneContract(MortgageSourceRecord input, DateTime assumedStartDate)
	{
		var endDate = input.Start_Date.AddYears(input.Term);
		var monthsDifference = (endDate.Year - assumedStartDate.Year) * 12 + endDate.Month - assumedStartDate.Month;
		var output = new MortgageOutputRecord[monthsDifference];
		MortgageOutputRecord nextOutput = new MortgageOutputRecord{Risk_Indicator = -1};
		var currentDate = assumedStartDate;
		int i = 0;
		while (currentDate.Year < endDate.Year && currentDate.Month <= endDate.Month)
		{
			nextOutput = CalculateOneStep(currentDate, nextOutput, input);
			output[i++] = nextOutput;
			currentDate = currentDate.AddMonths(1);
			if (nextOutput.Risk_Indicator == 5)
			{
				break;
			}
		}
		return output;
	}

	private MortgageOutputRecord CalculateOneStep(DateTime currentDate, MortgageOutputRecord nextOutput, MortgageSourceRecord ogRecord)
	{
		if (nextOutput.Risk_Indicator == -1)
		{
			nextOutput = new MortgageOutputRecord{
				Risk_Indicator = ogRecord.Risk_Indicator,
				Interest_Rate = ogRecord.Interest_Rate,
				Next_Reset_Date = ogRecord.Next_Reset_Date,
				Remaining_Notional = ogRecord.Remaining_Notional,
				Reset_Frequency = ogRecord.Reset_Frequency,
				Id = ogRecord.Id,
				Date_Of_Payment = currentDate,
				monthly_payment = GetMonthlyPayment(ogRecord.Payment_Type, ogRecord.Notional, ogRecord.Interest_Rate, ogRecord.Term),
				Repayment_Payment = 0.0f,
				Interest_Payment = 0.0f,
				WriteOff = 0.0f
			};
		}
		int newRiskIndicator = nextOutput.Risk_Indicator;
		int newResetFrequency = nextOutput.Reset_Frequency;
		float newInterestRate = nextOutput.Interest_Rate;
		float newRemainingNotional = nextOutput.Remaining_Notional;
		float newMonthlyPayment = nextOutput.monthly_payment;
		DateTime newNextResetDate = nextOutput.Next_Reset_Date;

		float roll = (float)Random.Shared.NextDouble();
		(var down, var up) = RiskMigration[nextOutput.Risk_Indicator];
		if (roll < down)
		{
			newRiskIndicator = nextOutput.Risk_Indicator - 1;
		}
		else if (roll > up)
		{
			newRiskIndicator = nextOutput.Risk_Indicator + 1;
		}
		//migrate risk if reset date is reached
		if (currentDate.Month >= nextOutput.Next_Reset_Date.Month && currentDate.Year >= nextOutput.Next_Reset_Date.Year)
		{
			int yearsleft = ogRecord.Term - ogRecord.Reset_Frequency;
			newResetFrequency = new[] {30, 25, 20, 15, 10, 9, 7, 5}.Where(x => x >= yearsleft).Min();
			newNextResetDate = newNextResetDate.AddYears(newResetFrequency);
			newInterestRate = GetInterestRate(ogRecord.Interest_Rate, ogRecord.Risk_Indicator, newRiskIndicator, ogRecord.Reset_Frequency, newResetFrequency);
			if (ogRecord.Payment_Type == "Annuity")
			{
				newMonthlyPayment = GetMonthlyPayment(ogRecord.Payment_Type, nextOutput.Remaining_Notional, newInterestRate, yearsleft);
			}
		}

		var r = newInterestRate / 12 / 100;
		var endDate = ogRecord.Start_Date.AddYears(ogRecord.Term);
		(float Repay, float Interest, float WriteOff) = ogRecord.Payment_Type switch
		{
			"Bullet" => currentDate.Year == endDate.Year && currentDate.Month == endDate.Month ? (newRemainingNotional, newRemainingNotional*r, 0.0f ) : (0.0f, newRemainingNotional*r, 0.0f),
			"Linear" => (nextOutput.monthly_payment, newRemainingNotional*r, 0.0f),
			"Annuity" => (nextOutput.monthly_payment - newRemainingNotional*r, newRemainingNotional*r, 0.0f),
			_ => throw new BruhMoment("Invalid payment type"),
		};
		if (newRiskIndicator == 5)
		{
			Repay = 0.0f;
			Interest = 0.0f;
			WriteOff = newRemainingNotional;
			newRemainingNotional = 0.0f;
		}
		else
		{
			newRemainingNotional -= Repay;
		}
		if (newRemainingNotional < 0.0f)
		{
			Repay = nextOutput.Remaining_Notional;
			newRemainingNotional = 0.0f;
		}

		return new MortgageOutputRecord{
			Id=nextOutput.Id,
			Interest_Rate=newInterestRate,
			Next_Reset_Date=newNextResetDate,
			Remaining_Notional=newRemainingNotional,
			Reset_Frequency=newResetFrequency,
			Date_Of_Payment=currentDate,
			monthly_payment=newMonthlyPayment,
			Repayment_Payment=Repay,
			Interest_Payment=Interest,
			WriteOff=WriteOff,
			Risk_Indicator=newRiskIndicator,
		};
	}

	private float GetMonthlyPayment(string paymentType, float Notional, float interestRate, int term)
	{
		var r = interestRate / 12 / 100;
		return paymentType switch
		{
			"Bullet" => 0.0f,
			"Linear" => Notional / (term * 12),
			"Annuity" => (Notional * r * (float)Math.Pow(1 + r, term * 12)) / ((float)Math.Pow(1 + r, term * 12) - 1),
			_ => throw new BruhMoment("Invalid payment type"),
		};
	}

	private float GetInterestRate(float oldRate, int oldRiskIndicator, int newRiskIndicator, int oldReset, int newReset)
	{
		float impliedBaseRate = oldRate - RiskSpread[oldRiskIndicator] - DurationSpread[oldReset];
		return impliedBaseRate + RiskSpread[newRiskIndicator] + DurationSpread[newReset];
	}
}

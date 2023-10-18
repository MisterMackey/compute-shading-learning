using Microsoft.Extensions.Hosting;

namespace CsharpSolution;

public class Worker : IHostedService
{
	private readonly MortgagePaymentCalculator calculator;
	private readonly IHostApplicationLifetime appLifetime;

	public Worker(MortgagePaymentCalculator calculator, IHostApplicationLifetime appLifetime)
	{
		this.calculator = calculator;
		this.appLifetime = appLifetime;
	}
	public async Task StartAsync(CancellationToken cancellationToken)
	{
		await calculator.Run();
		appLifetime.StopApplication();
	}

	public Task StopAsync(CancellationToken cancellationToken)
	{
		return Task.CompletedTask;
	}
}

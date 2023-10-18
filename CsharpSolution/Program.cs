using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using CsharpSolution;
using Microsoft.Extensions.Logging;

var builder = Host.CreateDefaultBuilder(args);
builder.ConfigureServices(services =>
{
    services.AddHostedService<Worker>();
    services.AddLogging(config => {
	config.ClearProviders();
	config.AddSimpleConsole();
    });
    services.AddTransient<MortgagePaymentCalculator>();
});
await builder.Build().RunAsync();

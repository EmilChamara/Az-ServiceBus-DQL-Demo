using System.Text.Json;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace Azure.ServiceBus.ConsumerApp;

class Program
{
    static async Task Main(string[] args)
    {
        var cfg = LoadConfiguration();
        var conn = cfg["ServiceBus:ConnectionString"]!;
        var topic = cfg["ServiceBus:TopicName"]!;
        var subscription = cfg["ServiceBus:SubscriptionName"]!;

        var client = new ServiceBusClient(conn);
        var processor = client.CreateProcessor(topic, subscription, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            PrefetchCount = 50,
            MaxConcurrentCalls = 5
        });

        Console.WriteLine("== Azure Service Bus Consumer (Listener) ==");
        Console.WriteLine("Listening for messages... Press 'q' to quit.\n");

        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                var msg = args.Message;
                var body = JsonDocument.Parse(msg.Body);
                var amount = body.RootElement.GetProperty("Amount").GetDecimal();

                if (amount <= 0)
                {
                    await args.DeadLetterMessageAsync(msg, "ValidationError", "Amount must be greater than zero");
                    Console.WriteLine($"Dead-lettered {msg.MessageId}");
                    return;
                }

                Console.WriteLine($"Processed {msg.MessageId} (Amount={amount})");
                await args.CompleteMessageAsync(msg);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                await args.AbandonMessageAsync(args.Message);
            }
        };

        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"Processor error: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        while (Console.ReadKey(intercept: true).KeyChar != 'q') { }

        await processor.StopProcessingAsync();
        await processor.DisposeAsync();
        await client.DisposeAsync();

        Console.WriteLine("\nConsumer stopped.");
    }

    private static IConfiguration LoadConfiguration()
    {
        return new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddUserSecrets<Program>(optional: true)
            .Build();
    }
}
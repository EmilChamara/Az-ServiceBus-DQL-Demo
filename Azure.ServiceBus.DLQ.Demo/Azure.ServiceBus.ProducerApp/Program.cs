using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace Azure.ServiceBus.ProducerApp;

class Program
{
    static async Task Main(string[] args)
    {
        var config = LoadConfiguration();
        var conn = config["ServiceBus:ConnectionString"]
                   ?? throw new InvalidOperationException("Missing ServiceBus:ConnectionString");
        var topic = config["ServiceBus:TopicName"] ?? "dlq-demo-topic";

        await using var client = new ServiceBusClient(conn);
        var sender = client.CreateSender(topic);

        Console.WriteLine("****  Azure Service Bus Producer Demo ****");
        Console.WriteLine("Enter 'q' to quit.\n");

        while (true)
        {
            Console.Write("Enter OrderId (or 'q' to quit): ");
            var id = Console.ReadLine();
            if (string.Equals(id, "q", StringComparison.OrdinalIgnoreCase)) break;

            Console.Write("Enter Amount: ");
            var amountInput = Console.ReadLine();
            if (!decimal.TryParse(amountInput, out var amount)) amount = 0;

            var msg = new ServiceBusMessage(BinaryData.FromObjectAsJson(new
            {
                Kind = "OrderCreated",
                OrderId = id,
                Amount = amount
            }))
            {
                MessageId = $"order-{id}",
                CorrelationId = id
            };

            await sender.SendMessageAsync(msg);
            Console.WriteLine($"Sent message for order {id} (Amount={amount})");
        }

        await sender.DisposeAsync();
        Console.WriteLine("\nProducer stopped.");
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
using System.Text.Json;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace Azure.serviceBus.DLQListenerApp;

class Program
{
    private static ServiceBusSender? _quarantineSender;

    static async Task Main(string[] args)
    {
        var cfg = LoadConfiguration();

        var conn = cfg["ServiceBus:ConnectionString"]!;
        var topic = cfg["ServiceBus:TopicName"]!;
        var subscription = cfg["ServiceBus:SubscriptionName"]!;
        var quarantineQueue = cfg["ServiceBus:QuarantineQueueName"]!;

        var client = new ServiceBusClient(conn, new ServiceBusClientOptions
        {
            EnableCrossEntityTransactions = true
        });

        var dlqProcessor = client.CreateProcessor(topic, subscription, new ServiceBusProcessorOptions
        {
            SubQueue = SubQueue.DeadLetter,
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 2,
            PrefetchCount = 10
        });

        var topicSender = client.CreateSender(topic);
        _quarantineSender = client.CreateSender(quarantineQueue);

        Console.WriteLine("== Azure Service Bus DLQ Listener ==");
        Console.WriteLine("Monitoring DLQ for messages... Press 'q' to quit.\n");

        dlqProcessor.ProcessMessageAsync += async args =>
        {
            var msg = args.Message;
            Console.WriteLine($"DLQ detected: {msg.MessageId} ({msg.DeadLetterReason})");

            try
            {
                // Repair message
                var repaired = RepairMessage(msg);

                // Replay atomically (send to topic + complete from DLQ)
                using var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await topicSender.SendMessageAsync(repaired);
                await args.CompleteMessageAsync(msg);
                tx.Complete();

                Console.WriteLine($"Replayed {msg.MessageId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Replay failed for {msg.MessageId}: {ex.Message}");
                await MoveToQuarantine(msg, ex);
                await args.CompleteMessageAsync(msg);
            }
        };

        dlqProcessor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"DLQ listener error: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await dlqProcessor.StartProcessingAsync();

        while (Console.ReadKey(intercept: true).KeyChar != 'q')
        {
        }

        await dlqProcessor.StopProcessingAsync();
        await dlqProcessor.DisposeAsync();
        await topicSender.DisposeAsync();
        await _quarantineSender.DisposeAsync();
        await client.DisposeAsync();

        Console.WriteLine("\nDLQ Listener stopped.");
    }

    private static IConfiguration LoadConfiguration()
    {
        return new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddUserSecrets<Program>(optional: true)
            .Build();
    }

    private static ServiceBusMessage RepairMessage(ServiceBusReceivedMessage msg)
    {
        var doc = JsonDocument.Parse(msg.Body);
        var root = doc.RootElement;
        var amount = root.TryGetProperty("Amount", out var a) && a.GetDecimal() > 0 ? a.GetDecimal() : 99.99m;

        if (msg.MessageId.Contains("bad"))
            throw new InvalidOperationException("Simulated repair failure");

        var repaired = new
        {
            Kind = root.GetProperty("Kind").GetString(),
            OrderId = root.GetProperty("OrderId").GetString(),
            Amount = amount,
            RepairedUtc = DateTime.UtcNow
        };

        return new ServiceBusMessage(BinaryData.FromObjectAsJson(repaired))
        {
            MessageId = msg.MessageId,
            CorrelationId = msg.CorrelationId
        };
    }

    private static async Task MoveToQuarantine(ServiceBusReceivedMessage msg, Exception ex)
    {
        var quarantineMsg = new ServiceBusMessage(msg.Body)
        {
            MessageId = $"{msg.MessageId}-quarantine",
            Subject = "ReplayFailed",
            ApplicationProperties =
            {
                ["OriginalMessageId"] = msg.MessageId,
                ["DeadLetterReason"] = msg.DeadLetterReason ?? "Unknown",
                ["ReplayError"] = ex.Message
            }
        };

        await _quarantineSender!.SendMessageAsync(quarantineMsg);
        Console.WriteLine($"Moved {msg.MessageId} to quarantine queue.");
    }
}
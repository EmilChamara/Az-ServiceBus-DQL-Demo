using System.Text.Json;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace Azure.ServiceBus.DLQ.Topic.Demo;

class Program
{
    private static ServiceBusClient _client = null!;
    private static ServiceBusSender _topicSender = null!;
    private static ServiceBusReceiver _subReceiver = null!;
    private static ServiceBusReceiver _dlqReceiver = null!;

    // Config
    private static string _topic = null!;
    private static string _subscription = null!;


    static async Task Main()
    {
        var config = LoadConfiguration();

        var conn = config["ServiceBus:ConnectionString"]
                   ?? throw new InvalidOperationException("Missing ServiceBus:ConnectionString");
        _topic = config["ServiceBus:TopicName"] ?? "dlq-demo-topic";
        _subscription = config["ServiceBus:SubscriptionName"] ?? "processor-sub";

        _client = new ServiceBusClient(conn, new ServiceBusClientOptions() { EnableCrossEntityTransactions = true });
        _topicSender = _client.CreateSender(_topic);
        _subReceiver = _client.CreateReceiver(_topic, _subscription);
        _dlqReceiver = _client.CreateReceiver(_topic, _subscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        await SendDemoMessagesAsync();

        Console.WriteLine("\n=== Processing subscription ===");
        await DrainSubscriptionAsync();

        Console.WriteLine("\n=== Repair & replay from SUB DLQ ===");
        await RepairAndReplayFromDlqAsync();

        Console.WriteLine("\n=== Final drain (should see repaired msgs) ===");
        await DrainSubscriptionAsync();

        await DisposeAsync();
        Console.WriteLine("\nDone.");
    }

    private static async Task DisposeAsync()
    {
        await _topicSender.DisposeAsync();
        await _subReceiver.DisposeAsync();
        await _dlqReceiver.DisposeAsync();
        await _client.DisposeAsync();
    }

    private static async Task SendDemoMessagesAsync()
    {
        Console.WriteLine("=== Sending demo messages to TOPIC ===");
        await SendAsync("Good-001", new { Kind = "Good", Amount = 19.99m });
        await SendAsync("Poison-001", new { Kind = "Poison" /* Amount missing */ });
        await SendAsync("Retry-001", new { Kind = "Retry", Amount = 50.00m });
        Console.WriteLine("Sent.");
    }

    private static async Task DrainSubscriptionAsync()
    {
        while (true)
        {
            var batch = await _subReceiver.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(2));
            if (batch.Count == 0) break;

            foreach (var msg in batch)
                await HandleSubscriptionMessageAsync(msg);
        }
    }

    private static async Task RepairAndReplayFromDlqAsync()
    {
        while (true)
        {
            var dead = await _dlqReceiver.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(2));
            if (dead.Count == 0) break;

            foreach (var msg in dead)
                await HandleDlqMessageAsync(msg);
        }
    }

    private static async Task HandleSubscriptionMessageAsync(ServiceBusReceivedMessage msg)
    {
        var kind = msg.ApplicationProperties.GetValueOrDefault("Kind")?.ToString() ?? "Unknown";
        Console.WriteLine($"[SUB] {msg.MessageId} (DeliveryCount={msg.DeliveryCount}, Kind={kind})");

        try
        {
            if (msg.MessageId.StartsWith("Good"))
            {
                await Task.Delay(150);
                await _subReceiver.CompleteMessageAsync(msg);
                Console.WriteLine("  -> Completed");
            }
            else if (msg.MessageId.StartsWith("Poison"))
            {
                await _subReceiver.DeadLetterMessageAsync(msg, "ValidationError", "Missing Amount field");
                Console.WriteLine("  -> Dead-lettered (ValidationError)");
            }
            else if (msg.MessageId.StartsWith("Retry"))
            {
                if (msg.DeliveryCount < 3)
                {
                    Console.WriteLine("  -> Simulating transient error, Abandoning...");
                    await _subReceiver.AbandonMessageAsync(msg);
                }
                else
                {
                    Console.WriteLine("  -> Threshold reached, Abandon (broker will DLQ it)");
                    await _subReceiver.AbandonMessageAsync(msg);
                }
            }
            else
            {
                await _subReceiver.CompleteMessageAsync(msg);
                Console.WriteLine("  -> Completed (default)");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  !! Handler error: {ex.Message} (lock may expire → retry)");
        }
    }

    private static async Task HandleDlqMessageAsync(ServiceBusReceivedMessage msg)
    {
        Console.WriteLine(
            $"[DLQ] {msg.MessageId} | Reason={msg.DeadLetterReason} | Desc={msg.DeadLetterErrorDescription}");

        try
        {
            // 1) Repair payload (non-destructive)
            var repairedJson = EnsureAmount(msg.Body.ToString());

            // 2) Atomic replay: send to TOPIC + complete SUB DLQ in one transaction
            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var resend = new ServiceBusMessage(repairedJson)
                {
                    MessageId = msg.MessageId,
                    CorrelationId = msg.CorrelationId
                };
                foreach (var kv in msg.ApplicationProperties)
                    resend.ApplicationProperties[kv.Key] = kv.Value;

                await _topicSender.SendMessageAsync(resend);
                await _dlqReceiver.CompleteMessageAsync(msg);
                ts.Complete();
            }

            Console.WriteLine($"  -> Replayed {msg.MessageId} to topic {_topic}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  !! Failed to repair/replay {msg.MessageId}: {ex.Message}");
            // Optionally: send to a Quarantine queue and Complete DLQ to avoid loops.
        }
    }

    private static async Task SendAsync(string id, object body)
    {
        var json = JsonSerializer.Serialize(body);
        var msg = new ServiceBusMessage(json)
        {
            MessageId = id,
            CorrelationId = "topic-dlq-demo",
            ApplicationProperties =
            {
                ["Kind"] = body.GetType().GetProperty("Kind")?.GetValue(body)?.ToString() ?? "Unknown"
            }
        };

        await _topicSender.SendMessageAsync(msg);
    }

    private static string EnsureAmount(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var kind = root.TryGetProperty("Kind", out var k) ? k.GetString() ?? "Unknown" : "Unknown";
        var amount = root.TryGetProperty("Amount", out var a) && a.ValueKind == JsonValueKind.Number
            ? a.GetDecimal()
            : 9.99m;

        var repaired = new
        {
            Kind = kind,
            Amount = amount,
            IsRepaired = true,
            RepairedUtc = DateTime.UtcNow
        };
        return JsonSerializer.Serialize(repaired);
    }


    private static IConfiguration LoadConfiguration() =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddUserSecrets<Program>(optional: true)
            .AddEnvironmentVariables()
            .Build();
}
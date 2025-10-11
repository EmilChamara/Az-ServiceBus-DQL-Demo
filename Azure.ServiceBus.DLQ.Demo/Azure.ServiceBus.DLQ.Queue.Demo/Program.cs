using System.Text.Json;
using Azure.Messaging.ServiceBus;

namespace Azure.ServiceBus.DLQ.Queue.Demo;

using Microsoft.Extensions.Configuration;

class Program
{
    private static ServiceBusClient _client = null!;
    private static ServiceBusSender _sender = null!;
    private static ServiceBusReceiver _receiver = null!;
    private static ServiceBusReceiver _dlqReceiver = null!;
    private static string _queue = null!;

    static async Task Main()
    {
        var config = LoadConfiguration();

        var conn = config["ServiceBus:ConnectionString"]
                   ?? throw new InvalidOperationException("Missing ServiceBus:ConnectionString");

        _queue = config["ServiceBus:QueueName"] ?? "dlq-demo-queue";

        _client = new ServiceBusClient(conn);
        _sender = _client.CreateSender(_queue);
        _receiver = _client.CreateReceiver(_queue);
        _dlqReceiver = _client.CreateReceiver(_queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        await RunDemoAsync();

        await DisposeAsync();
        Console.WriteLine("\n Done.");
    }

    private static IConfiguration LoadConfiguration()
    {
        return new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddUserSecrets<Program>(optional: true)
            .AddEnvironmentVariables()
            .Build();
    }

    private static async Task RunDemoAsync()
    {
        await SendDemoMessagesAsync();

        Console.WriteLine("\n=== Processing main queue ===");
        await DrainMainQueueAsync();

        Console.WriteLine("\n=== Repair & replay from DLQ ===");
        await RepairAndReplayFromDlqAsync();

        Console.WriteLine("\n=== Final drain (should see repaired msgs) ===");
        await DrainMainQueueAsync();
    }

    private static async Task DisposeAsync()
    {
        await _sender.DisposeAsync();
        await _receiver.DisposeAsync();
        await _dlqReceiver.DisposeAsync();
        await _client.DisposeAsync();
    }

    private static async Task SendDemoMessagesAsync()
    {
        Console.WriteLine("=== Sending demo messages ===");
        await SendAsync("Good-001", new { Kind = "Good", Amount = 19.99m });
        await SendAsync("Poison-001", new { Kind = "Poison" });
        await SendAsync("Retry-001", new { Kind = "Retry", Amount = 50.00m });
        Console.WriteLine("Sent.");
    }

    private static async Task DrainMainQueueAsync()
    {
        while (true)
        {
            var batch = await _receiver.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(2));
            if (batch.Count == 0) break;
            foreach (var msg in batch)
                await HandleMainMessageAsync(msg);
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

    private static async Task HandleDlqMessageAsync(ServiceBusReceivedMessage msg)
    {
        Console.WriteLine(
            $"[DLQ] {msg.MessageId} | Reason={msg.DeadLetterReason} | Desc={msg.DeadLetterErrorDescription}");
        try
        {
            var repairedJson = EnsureAmount(msg.Body.ToString());
         

            var resend = new ServiceBusMessage(repairedJson)
            {
                MessageId = msg.MessageId,
                CorrelationId = msg.CorrelationId
            };
            foreach (var kv in msg.ApplicationProperties)
                resend.ApplicationProperties[kv.Key] = kv.Value;

            await _sender.SendMessageAsync(resend);
            await _dlqReceiver.CompleteMessageAsync(msg);

            Console.WriteLine($"  -> Replayed {msg.MessageId} back to {_queue}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  !! Failed to repair/replay {msg.MessageId}: {ex.Message}");
        }
    }

    #region Message Handlers

    private static async Task HandleMainMessageAsync(ServiceBusReceivedMessage msg)
    {
        var kind = msg.ApplicationProperties.GetValueOrDefault("Kind")?.ToString() ?? "Unknown";
        Console.WriteLine($"[MAIN] {msg.MessageId} (DeliveryCount={msg.DeliveryCount}, Kind={kind})");

        try
        {
            if (msg.MessageId.StartsWith("Good"))
            {
                await Task.Delay(150);
                await _receiver.CompleteMessageAsync(msg);
                Console.WriteLine("  -> Completed");
            }
            else if (msg.MessageId.StartsWith("Poison"))
            {
                await _receiver.DeadLetterMessageAsync(msg, "ValidationError", "Missing Amount field");
                Console.WriteLine("  -> Dead-lettered (ValidationError)");
            }
            else if (msg.MessageId.StartsWith("Retry"))
            {
                if (msg.DeliveryCount < 3)
                {
                    Console.WriteLine("  -> Simulating transient error, Abandoning...");
                    await _receiver.AbandonMessageAsync(msg);
                }
                else
                {
                    Console.WriteLine("  -> Threshold reached, Abandon (broker will DLQ it)");
                    await _receiver.AbandonMessageAsync(msg);
                }
            }
            else
            {
                await _receiver.CompleteMessageAsync(msg);
                Console.WriteLine("  -> Completed (default)");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  !! Handler error: {ex.Message} (lock may expire → retry)");
        }
    }

    #endregion

    #region Helpers

    private static async Task SendAsync(string id, object body)
    {
        var json = JsonSerializer.Serialize(body);
        var msg = new ServiceBusMessage(json)
        {
            MessageId = id,
            CorrelationId = "dlq-demo",
            ApplicationProperties =
            {
                ["Kind"] = body.GetType().GetProperty("Kind")?.GetValue(body)?.ToString() ?? "Unknown"
            }
        };

        await _sender.SendMessageAsync(msg);
    }

    private static string EnsureAmount(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        string kind = root.TryGetProperty("Kind", out var k) ? k.GetString() ?? "Unknown" : "Unknown";
        decimal amount = root.TryGetProperty("Amount", out var a) && a.ValueKind == JsonValueKind.Number
            ? a.GetDecimal()
            : 9.99m;

        var repaired = new
        {
            Kind = kind,
            Amount = amount,
            RepairedUtc = DateTime.UtcNow
        };
        return JsonSerializer.Serialize(repaired);
    }

    #endregion
}
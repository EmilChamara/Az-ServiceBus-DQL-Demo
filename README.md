# 🧩 Azure Service Bus Reliability Demo  
### Dead-Letter Queues (DLQ), Repair & Replay, and Quarantine Pattern  

This repository demonstrates how to build **reliable messaging flows** in **Azure Service Bus**, focusing on how to handle failures using **Dead-Letter Queues (DLQ)** and the **Quarantine pattern**.  

It’s part of the article series:  
> **“Building Reliable Messaging with Azure Service Bus” – Part 2: Dead-Letter Queues & Repair Pipelines**

---

## 🎥 Demo in Action  

https://github.com/EmilChamara/Az-ServiceBus-DQL-Demo/blob/main/azure-service-bus-demo.mp4 

The clip shows all three apps running together:  
- Producer publishing messages  
- Consumer processing and dead-lettering invalid ones  
- DLQ Listener automatically repairing, replaying, and quarantining failures  

---

## 🏗 Project Structure  

| Project | Description |
|----------|--------------|
| **Azure.ServiceBus.ProducerApp** | Sends sample messages to a Service Bus **Topic**. |
| **Azure.ServiceBus.ConsumerApp** | Listens on a **Subscription**, processes valid messages, and sends bad ones to the **DLQ**. |
| **Azure.ServiceBus.DLQListnerApp** | Monitors the **DLQ**, repairs failed messages, replays them, and moves unrecoverable ones to a **Quarantine Queue**. |

---

## ⚙️ Prerequisites  

- .NET 8 SDK or later  
- An **Azure Service Bus Namespace**  
- A Service Bus **Topic**, **Subscription**, and a **Queue** named `quarantine`  
- Connection string with `Send`, `Listen`, and `Manage` rights  

---

## 🔧 Configuration  

Each project contains an `appsettings.json`:  

```json
{
  "ServiceBus": {
    "ConnectionString": "<your-service-bus-connection-string>",
    "TopicName": "reliability-demo-topic",
    "SubscriptionName": "processor-sub",
    "QuarantineQueueName": "quarantine"
  }
}

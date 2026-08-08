# Kafka Fundamentals — Complete Reference Notes

## How to use these notes

Before learning Kafka internals or troubleshooting Kafka issues, you should first understand these fundamentals:

1. Why Kafka is needed
2. Producer
3. Consumer
4. Topic
5. Partition
6. Offset
7. Consumer Group
8. Consumer Lag
9. Retention

Once these are clear, move to:

```text
Kafka Architecture
      ↓
Broker
      ↓
Cluster
      ↓
Leader / Follower
      ↓
Replication
      ↓
ISR
      ↓
Producer Internals
      ↓
Acknowledgements
      ↓
Timeouts
      ↓
Spring Kafka
      ↓
Production Troubleshooting
```

---

# 1. Why Do We Need Kafka?

The fundamental problem Kafka solves is communication between distributed applications at scale.

Imagine a company has multiple Spring Boot microservices:

```text
Order Service
Payment Service
Inventory Service
Notification Service
Analytics Service
```

When an order is created, several services may need to know about it.

## Without Kafka

```text
                    ┌──→ Payment
                    │
Order Service ──────┼──→ Inventory
                    │
                    ├──→ Notification
                    │
                    └──→ Analytics
```

This creates:

- Tight coupling
- Direct dependencies between services
- Difficult failure handling
- Difficult scaling
- Difficult handling of traffic spikes
- Producer needs to know about consumers

## With Kafka

```text
Order Service
      │
      │ OrderCreated
      ↓
    Kafka
      │
      ├──→ Payment
      ├──→ Inventory
      ├──→ Notification
      └──→ Analytics
```

The Order Service only needs to know:

> "Publish `OrderCreated` to Kafka."

It does not need to know who consumes it.

This is called **decoupling**.

---

# 2. What Is Kafka?

A simple definition:

> **Apache Kafka is a distributed event-streaming platform used to reliably publish, store, and consume large amounts of events/records at scale.**

For now, think of Kafka as:

```text
A highly scalable and durable middle layer between producers and consumers.
```

Kafka provides concepts such as:

- Persistence
- Scalability
- Partitioning
- Replication
- Consumer groups
- Retention
- Fault tolerance
- High throughput

---

# 3. Producer

## What is a Producer?

A **Producer** is a component or application that **publishes records to Kafka**.

Example:

```text
Order Service
      │
      │ OrderCreated
      ↓
    Kafka
```

Here:

```text
Order Service = Producer
```

The producer does not normally send the message directly to a consumer.

Instead:

```text
Producer
   │
   │ WRITE
   ↓
Kafka Topic
   ↑
   │ READ
Consumer
```

### Spring Boot example

You may see:

```java
kafkaTemplate.send("orders", order);
```

Conceptually:

```text
kafkaTemplate → Kafka producer
"orders"      → Topic
order         → Record
```

### Producer responsibility

```text
Application
    ↓
Create record
    ↓
Kafka Producer
    ↓
Send record to Kafka
```

### Remember

> **Producer = writes/publishes records to Kafka.**

---

# 4. Consumer

## What is a Consumer?

A **Consumer** is a component or application that **reads records from Kafka**.

Example:

```text
Kafka
  │
  │ OrderCreated
  ↓
Notification Service
```

Here:

```text
Notification Service = Consumer
```

The consumer reads the record and performs some business operation.

For example:

```text
Kafka
  ↓
OrderCreated
  ↓
Notification Service
  ↓
Send notification
```

### Producer vs Consumer

```text
Producer
   ↓
   WRITE
   ↓
 Kafka Topic
   ↑
   │ READ
   ↑
Consumer
```

### Important

A consumer does not necessarily delete a record from Kafka after reading it.

Kafka keeps records according to the topic's retention policy.

### Remember

> **Consumer = reads/consumes records from Kafka.**

---

# 5. Topic

## What is a Topic?

A **Topic** is a named stream/category where Kafka records are published.

Examples:

```text
orders
payments
notifications
shipments
```

For example:

```text
orders topic

OrderCreated
OrderUpdated
OrderCancelled
```

A producer might publish:

```text
Order Service
      │
      │ OrderCreated
      ↓
orders topic
```

Consumers interested in orders can read from that topic.

## Important

A topic is divided into partitions:

```text
orders topic
     │
     ├── Partition 0
     ├── Partition 1
     ├── Partition 2
     └── Partition 3
```

### Remember

> **Topic = named stream/category to which producers publish records and consumers read records.**

---

# 6. Partition

## What is a Partition?

A **Partition** is an ordered sequence of records inside a Kafka topic.

Example:

```text
orders topic

Partition 0
Partition 1
Partition 2
Partition 3
```

A partition contains records in order:

```text
Partition 0

M1 → M2 → M3 → M4 → M5
```

## Why do we need partitions?

The main reasons are:

1. Scalability
2. Parallel processing
3. Distribution of data

For example:

```text
P0 → Consumer 1
P1 → Consumer 2
P2 → Consumer 3
P3 → Consumer 4
```

Different partitions can be processed in parallel.

## Ordering

Kafka maintains ordering **within a partition**.

```text
P0:

M1 → M2 → M3 → M4
```

A consumer reading that partition sees records in that order.

However, do not assume a global ordering across multiple partitions.

For example:

```text
P0: M1 → M3

P1: M2 → M4
```

You should not assume the overall order is:

```text
M1 → M2 → M3 → M4
```

### Remember

> **Partition = an ordered sequence of records inside a topic that enables scalability and parallel processing.**

---

# 7. Offset

## What is an Offset?

An **Offset** represents the position of a record within a partition.

Example:

```text
Partition 0

Offset:
  0     1     2     3     4
  ↓     ↓     ↓     ↓     ↓
 M1    M2    M3    M4    M5
```

Therefore:

```text
M1 → offset 0
M2 → offset 1
M3 → offset 2
M4 → offset 3
M5 → offset 4
```

## Why is an offset needed?

The consumer needs a way to track its progress through the records.

Suppose the consumer has processed:

```text
M1
M2
M3
```

The consumer group can commit its progress so that after a restart it knows where it should continue.

## Important

An offset is **not a global message ID for the entire Kafka cluster**.

Offsets are specific to a partition.

For example:

```text
Partition 0:
M1 → offset 0
M2 → offset 1

Partition 1:
M3 → offset 0
M4 → offset 1
```

Both partitions can have offset `0`.

### Remember

> **Offset = the position of a record within a partition, used to track consumer progress.**

---

# 8. Consumer Group

## What is a Consumer Group?

A **Consumer Group** is a group of consumers that work together to consume a topic.

Suppose Notification Service has:

```text
Notification Instance 1
Notification Instance 2
Notification Instance 3
```

They can belong to:

```text
Consumer Group: notification-service
```

```text
       Notification Group
          │    │    │
          C1   C2   C3
```

If the topic has three partitions:

```text
P0
P1
P2
```

Kafka can distribute them among the consumers:

```text
P0 → C1
P1 → C2
P2 → C3
```

Now the three instances share the workload.

## Why do consumer groups matter?

Suppose:

```text
Producer = 100,000 messages/sec
```

and one consumer can process:

```text
20,000 messages/sec
```

We can have:

```text
C1 → 20K
C2 → 20K
C3 → 20K
C4 → 20K
C5 → 20K
```

Total:

```text
100,000 messages/sec
```

This allows horizontal scaling.

---

# 9. Different Consumer Groups

Suppose the same `OrderCreated` event is needed by:

```text
Payment Service
Inventory Service
Notification Service
Analytics Service
```

Each can have a different consumer group:

```text
                 Kafka
                   │
              OrderCreated
                   │
       ┌───────────┼───────────┐
       ↓           ↓           ↓
    Payment     Inventory   Notification
     Group        Group        Group
```

Each group can consume the same event independently.

### Remember

> **Different consumer groups = independent consumption.**

---

# 10. Same Consumer Group vs Different Consumer Groups

This is a **must-remember Kafka concept**.

## Same Consumer Group

```text
Notification Group

C1
C2
C3
```

Purpose:

> **Share the workload.**

## Different Consumer Groups

```text
Payment Group
Inventory Group
Notification Group
```

Purpose:

> **Different applications independently consume the same event.**

### Memorize

```text
SAME GROUP
    ↓
Work sharing

DIFFERENT GROUPS
    ↓
Independent consumption
```

---

# 11. Consumer Lag

## What is Consumer Lag?

**Consumer Lag** represents how far behind a consumer group is compared with the latest records available in Kafka.

Suppose Kafka has:

```text
100,000 records
```

and the consumer has processed:

```text
80,000
```

Then approximately:

```text
Lag = 100,000 - 80,000
    = 20,000
```

Conceptually:

```text
Kafka:

M1 M2 M3 M4 M5 M6 M7 M8 M9 M10
                         ↑
                    Latest record

Consumer:

M1 M2 M3 M4 M5 M6
               ↑
          Consumer progress
```

The gap represents lag.

## Why does lag increase?

If:

```text
Producer = 100K/sec
Consumer = 20K/sec
```

then:

```text
Producer > Consumer
       ↓
Consumer Lag ↑
```

Example:

```text
After 1 sec → ~80K lag
After 2 sec → ~160K lag
After 3 sec → ~240K lag
```

## Why is lag important?

As a Platform Engineer, if a team says:

> "Our Kafka consumer is slow."

Consumer lag is one of the first things to investigate.

Increasing lag could indicate:

- Consumer processing is slow
- Consumer instances are insufficient
- Database is slow
- Downstream services are slow
- Consumer errors/retries
- Insufficient partitions/parallelism
- CPU/memory constraints

### Remember

> **Consumer lag = how far behind a consumer group is from the latest available records.**

---

# 12. Retention

## What is Retention?

Kafka does **not normally delete a record immediately after a consumer reads it**.

Instead, Kafka retains records according to the topic's **retention configuration**.

For example:

```text
Topic retention = 7 days
```

Conceptually:

```text
Producer
   ↓
Kafka
   ↓
M1 M2 M3 M4 M5
```

The consumer reads:

```text
M1
M2
```

But Kafka can still retain those records.

The retention policy determines when records become eligible for deletion.

## Why is retention useful?

Suppose:

```text
Notification Service
       ↓
      DOWN
```

The producer continues publishing:

```text
M1
M2
M3
M4
M5
```

Kafka retains them.

Later:

```text
Notification Service
       ↓
      UP
       ↓
Consumes available records
```

As long as the records are still retained and available to that consumer group, they can be consumed.

## Important

Kafka does not simply say:

> "Consumer read M1, therefore delete M1."

Instead, records are retained according to the topic's retention configuration.

This is also why another consumer group can potentially consume the same record independently.

### Remember

> **Retention = how long Kafka keeps records before they become eligible for deletion.**

---

# 13. What Happens If a Consumer Is Down?

Suppose:

```text
Producer → Kafka → Notification Service
```

and Notification Service goes down.

The producer can continue publishing:

```text
M1
M2
M3
```

Kafka can retain those records.

```text
Kafka Topic

┌──────┬──────┬──────┐
│ M1   │ M2   │ M3   │
└──────┴──────┴──────┘

Notification Service
       ❌ DOWN
```

When Notification Service comes back:

```text
Notification Service
       ↓
     comes back
       ↓
reads available records
```

It can continue from the appropriate committed consumer-group position, assuming the records are still retained.

---

# 14. What If Messages Arrive While the Consumer Is Down?

Suppose:

```text
M1 → Consumer DOWN
M2 → Consumer DOWN
M3 → Consumer UP
```

Kafka may contain:

```text
┌──────┬──────┬──────┐
│ M1   │ M2   │ M3   │
└──────┴──────┴──────┘
```

When the consumer comes back, it does not simply start with M3.

It normally continues from the appropriate consumer-group offset.

So it can process:

```text
M1 → M2 → M3
```

assuming the records are still retained and the consumer group has not already advanced past them.

---

# 15. Kafka Solves the "Too Many Requests" Problem

Imagine:

```text
Producer = 100,000 messages/sec
Consumer = 20,000 messages/sec
```

The consumer cannot process everything immediately.

Kafka provides a durable buffer:

```text
Producer
100,000/sec
     ↓
   Kafka
     ↓
Consumer
20,000/sec
```

After one second:

```text
Produced = 100,000
Consumed = 20,000

Backlog ≈ 80,000
```

If the producer continues at 100K/sec and consumers remain at 20K/sec, lag continues to grow.

## Important

Kafka does **not** magically increase consumer capacity.

If:

```text
Production rate > Consumption rate
```

then:

```text
Consumer Lag ↑
```

You need to increase consumer capacity or reduce the incoming rate.

---

# 16. Scaling Consumers

Suppose one consumer processes:

```text
20,000 messages/sec
```

Deploy five consumers:

```text
Consumer 1 → 20K
Consumer 2 → 20K
Consumer 3 → 20K
Consumer 4 → 20K
Consumer 5 → 20K
```

Total:

```text
5 × 20,000
= 100,000 messages/sec
```

Now consumer capacity matches producer throughput.

```text
Producer
100K/sec
   ↓
 Kafka
   ↓
 ┌───┬───┬───┬───┬───┐
 ↓   ↓   ↓   ↓   ↓
 C1  C2  C3  C4  C5
20K 20K 20K 20K 20K
```

Partitions allow this work to be performed in parallel.

---

# 17. Kafka Solves the "Many Consumers" Problem

Suppose Order Service creates:

```text
OrderCreated
```

and four different services need to know about it:

```text
Payment Service
Inventory Service
Notification Service
Analytics Service
```

Without Kafka:

```text
                    ┌──→ Payment
                    │
Order Service ──────┼──→ Inventory
                    │
                    ├──→ Notification
                    │
                    └──→ Analytics
```

With Kafka:

```text
Order Service
      │
      │ OrderCreated
      ↓
    Kafka
      │
      ├──→ Payment Group
      ├──→ Inventory Group
      ├──→ Notification Group
      └──→ Analytics Group
```

The producer publishes the event once.

Different consumer groups can consume it independently.

---

# 18. Complete Kafka Mental Model

Use this diagram whenever you need to refresh the basics:

```text
                         PRODUCER
                            │
                            │ writes records
                            ↓
                    ┌──────────────┐
                    │    TOPIC     │
                    │              │
                    │ P0 P1 P2 P3  │
                    └──────┬───────┘
                           │
                           ↓
                      PARTITIONS
                           │
                           ↓
                         OFFSETS
                           │
                           ↓
                    CONSUMER GROUP
                     /      |      \
                    C1      C2      C3
                     \      |      /
                      \     |     /
                       \    |    /
                        CONSUME
                           │
                           ↓
                    Consumer Lag
                           │
                           ↓
                 Records retained according
                    to Retention Policy
```

---

# 19. Complete Relationship Between the Concepts

Think about the concepts in this order:

```text
PRODUCER
   │
   │ writes records
   ↓
TOPIC
   │
   │ divided into
   ↓
PARTITIONS
   │
   │ each record has
   ↓
OFFSET
   │
   │ consumed by
   ↓
CONSUMER
   │
   │ organized into
   ↓
CONSUMER GROUP
   │
   │ if unable to keep up
   ↓
CONSUMER LAG
   │
   │ while records are kept according to
   ↓
RETENTION
```

---

# 20. Quick Revision Table

| Concept | Simple Meaning | Main Purpose |
|---|---|---|
| **Producer** | Writes records to Kafka | Publish events |
| **Consumer** | Reads records from Kafka | Process events |
| **Topic** | Named stream/category of records | Organize events |
| **Partition** | Ordered sequence inside a topic | Scalability + parallelism |
| **Offset** | Position of a record in a partition | Track consumption progress |
| **Consumer Group** | Group of consumers working together | Load sharing + independent consumption |
| **Consumer Lag** | How far behind a consumer group is | Monitor processing backlog |
| **Retention** | How long Kafka keeps records | Durability + replay/recovery |

---

# 21. Prerequisite Checklist

Before moving to Kafka architecture, you should be able to explain these **without looking at your notes**.

## Level 1 — Basic Participants

- [ ] What is a Producer?
- [ ] What is a Consumer?

## Level 2 — Data Organization

- [ ] What is a Topic?
- [ ] What is a Partition?
- [ ] Why does Kafka need partitions?
- [ ] What is an Offset?

## Level 3 — Consumption

- [ ] What is a Consumer Group?
- [ ] Same group vs different groups
- [ ] Why do we need consumer groups?

## Level 4 — Operations

- [ ] What is Consumer Lag?
- [ ] Why does lag increase?
- [ ] What is Retention?
- [ ] What happens when a consumer goes down?

## Level 5 — Big Picture

You should be able to explain:

```text
Producer
   ↓
Topic
   ↓
Partition
   ↓
Offset
   ↓
Consumer Group
   ↓
Consumer
   ↓
Consumer Lag
   ↓
Retention
```

---

# 22. What to Learn Next

Only after these fundamentals are clear, move to:

```text
Kafka Broker
      ↓
Kafka Cluster
      ↓
Partitions on Brokers
      ↓
Partition Leader
      ↓
Partition Followers
      ↓
Replication
      ↓
ISR (In-Sync Replicas)
      ↓
Producer Request Flow
      ↓
Acknowledgements (acks)
      ↓
Retries
      ↓
Producer Timeouts
      ↓
Spring Kafka
      ↓
Production Troubleshooting
```

This next section is especially important for troubleshooting intermittent **Kafka Producer `TimeoutException`** in Spring Boot services.

---

# ⭐ Final 10 Concepts to Memorize

Before moving ahead, make sure you can explain:

1. **Why Kafka is needed**
2. **Producer**
3. **Consumer**
4. **Topic**
5. **Partition**
6. **Offset**
7. **Consumer Group**
8. **Consumer Lag**
9. **Retention**
10. **Same Consumer Group vs Different Consumer Groups**

The most important mental model is:

```text
Producer
   ↓
Topic
   ↓
Partitions
   ↓
Offsets
   ↓
Consumer Groups
   ↓
Consumers
   ↓
Consumer Lag
   ↓
Retention
```

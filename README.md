## A Kafka app which tests consumer assignment.

Spark uses the following pattern to find out partition assignment:
```
    consumer.poll(0)
    val partitions = consumer.assignment()
```
This approach has 2 problems:
 * It waits infinitely for metadata update (which ends-up in a live lock when the broker is down right at the beginning)
 * This API is deprecated since Kafka 2.0.0 (please see [Documentation](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll-long-))

However there is an alternative which applies the timeout for metadata update as well:
```
    consumer.poll(Duration.ZERO)
    val partitions = consumer.assignment()
```

In the mentioned code the `poll` triggers Kafka to do the assignment and `assignment` API gives this information back.

* In the first case since metadata update happens synchronously `assignment` API can give back the proper data.
* The second case is different because metadata update is triggered but highly probable
  that `assignment` API can't give back the proper data in the first round.
  Because of this the mentioned 2 lines has to be wrapped with a loop (and timeout in Spark's code).

Here is a log snippet where assignment triggered but the proper data arrives only a couple of rounds later:
```
>>> 19/07/12 11:01:39 INFO utils.AppInfoParser: Kafka version: 2.3.0
>>> 19/07/12 11:01:39 INFO utils.AppInfoParser: Kafka commitId: 05fcfde8f69b0349
>>> 19/07/12 11:01:39 INFO consumer.KafkaConsumer: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] Subscribed to topic(s): ceb36686-49f2-4a8f-9956-fa833a5c019e
>>> 19/07/12 11:01:39 INFO topicstress.KafkaGetAssigment$: OK
>>> 19/07/12 11:01:39 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:40 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:41 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:42 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:43 INFO clients.Metadata: Cluster ID: PdLJ-8qNRICoPp9EqKShGA
>>> 19/07/12 11:01:43 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:43 INFO zk.AdminZkClient: Creating topic __consumer_offsets with configuration {segment.bytes=104857600, compression.type=producer, cleanup.policy=compact} and initial partition assignment Map(0 -> ArrayBuffer(0))
>>> 19/07/12 11:01:43 INFO server.PrepRequestProcessor: Got user-level KeeperException when processing sessionid:0x1008fadde4a0000 type:setData cxid:0x4b zxid:0x23 txntype:-1 reqpath:n/a Error Path:/config/topics/__consumer_offsets Error:KeeperErrorCode = NoNode for /config/topics/__consumer_offsets
>>> 19/07/12 11:01:43 INFO server.KafkaApis: [KafkaApi-0] Auto creation of topic __consumer_offsets with 1 partitions and replication factor 1 is successful
>>> 19/07/12 11:01:43 INFO controller.KafkaController: [Controller id=0] New topics: [Set(__consumer_offsets)], deleted topics: [Set()], new partition replica assignment [Map(__consumer_offsets-0 -> Vector(0))]
>>> 19/07/12 11:01:43 INFO controller.KafkaController: [Controller id=0] New partition creation callback for __consumer_offsets-0
>>> 19/07/12 11:01:43 INFO server.ReplicaFetcherManager: [ReplicaFetcherManager on broker 0] Removed fetcher for partitions Set(__consumer_offsets-0)
>>> 19/07/12 11:01:43 INFO log.Log: [Log partition=__consumer_offsets-0, dir=/private/var/folders/t_/w90m85fn2gjb1v0c24wrfrxm0000gp/T/spark-d0a3facb-a673-45ba-9d1d-64fbf331f227] Loading producer state till offset 0 with message format version 2
>>> 19/07/12 11:01:43 INFO log.Log: [Log partition=__consumer_offsets-0, dir=/private/var/folders/t_/w90m85fn2gjb1v0c24wrfrxm0000gp/T/spark-d0a3facb-a673-45ba-9d1d-64fbf331f227] Completed load of log with 1 segments, log start offset 0 and log end offset 0 in 3 ms
>>> 19/07/12 11:01:43 INFO log.LogManager: Created log for partition __consumer_offsets-0 in /private/var/folders/t_/w90m85fn2gjb1v0c24wrfrxm0000gp/T/spark-d0a3facb-a673-45ba-9d1d-64fbf331f227 with properties {compression.type -> producer, message.format.version -> 2.2-IV1, file.delete.delay.ms -> 60000, max.message.bytes -> 1000012, min.compaction.lag.ms -> 0, message.timestamp.type -> CreateTime, message.downconversion.enable -> true, min.insync.replicas -> 1, segment.jitter.ms -> 0, preallocate -> false, min.cleanable.dirty.ratio -> 0.5, index.interval.bytes -> 4096, unclean.leader.election.enable -> false, retention.bytes -> -1, delete.retention.ms -> 86400000, cleanup.policy -> compact, flush.ms -> 9223372036854775807, segment.ms -> 604800000, segment.bytes -> 104857600, retention.ms -> 604800000, message.timestamp.difference.max.ms -> 9223372036854775807, segment.index.bytes -> 10485760, flush.messages -> 1}.
>>> 19/07/12 11:01:43 INFO cluster.Partition: [Partition __consumer_offsets-0 broker=0] No checkpointed highwatermark is found for partition __consumer_offsets-0
>>> 19/07/12 11:01:43 INFO cluster.Replica: Replica loaded for partition __consumer_offsets-0 with initial high watermark 0
>>> 19/07/12 11:01:43 INFO cluster.Partition: [Partition __consumer_offsets-0 broker=0] __consumer_offsets-0 starts at Leader Epoch 0 from offset 0. Previous Leader Epoch was: -1
>>> 19/07/12 11:01:43 INFO group.GroupMetadataManager: [GroupMetadataManager brokerId=0] Scheduling loading of offsets and group metadata from __consumer_offsets-0
>>> 19/07/12 11:01:43 INFO group.GroupMetadataManager: [GroupMetadataManager brokerId=0] Finished loading offsets and group metadata from __consumer_offsets-0 in 5 milliseconds.
>>> 19/07/12 11:01:44 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:45 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:46 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:47 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:48 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:49 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:50 INFO internals.AbstractCoordinator: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] Discovered group coordinator 127.0.0.1:63475 (id: 2147483647 rack: null)
>>> 19/07/12 11:01:50 INFO internals.ConsumerCoordinator: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] Revoking previously assigned partitions []
>>> 19/07/12 11:01:50 INFO internals.AbstractCoordinator: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] (Re-)joining group
>>> 19/07/12 11:01:50 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:51 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:52 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:53 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:54 INFO internals.AbstractCoordinator: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] (Re-)joining group
>>> 19/07/12 11:01:54 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:54 INFO group.GroupCoordinator: [GroupCoordinator 0]: Preparing to rebalance group consumer-static-group-id in state PreparingRebalance with old generation 0 (__consumer_offsets-0) (reason: Adding new member consumer-1-05b77f73-f61d-496e-b4a0-97ffd4ea2b22)
>>> 19/07/12 11:01:54 INFO group.GroupCoordinator: [GroupCoordinator 0]: Stabilized group consumer-static-group-id generation 1 (__consumer_offsets-0)
>>> 19/07/12 11:01:55 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:56 INFO topicstress.KafkaGetAssigment$: Assigment: []
>>> 19/07/12 11:01:56 INFO group.GroupCoordinator: [GroupCoordinator 0]: Assignment received from leader for group consumer-static-group-id for generation 1
>>> 19/07/12 11:01:57 INFO internals.AbstractCoordinator: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] Successfully joined group with generation 1
>>> 19/07/12 11:01:57 INFO internals.ConsumerCoordinator: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] Setting newly assigned partitions: ceb36686-49f2-4a8f-9956-fa833a5c019e-0
>>> 19/07/12 11:01:57 INFO topicstress.KafkaGetAssigment$: Assigment: [ceb36686-49f2-4a8f-9956-fa833a5c019e-0]
>>> 19/07/12 11:01:58 INFO topicstress.KafkaGetAssigment$: Assigment: [ceb36686-49f2-4a8f-9956-fa833a5c019e-0]
>>> 19/07/12 11:01:59 INFO internals.Fetcher: [Consumer clientId=consumer-1, groupId=consumer-static-group-id] Resetting offset for partition ceb36686-49f2-4a8f-9956-fa833a5c019e-0 to offset 20.
>>> 19/07/12 11:01:59 INFO topicstress.KafkaGetAssigment$: Assigment: [ceb36686-49f2-4a8f-9956-fa833a5c019e-0]
>>> 19/07/12 11:02:01 INFO topicstress.KafkaGetAssigment$: Assigment: [ceb36686-49f2-4a8f-9956-fa833a5c019e-0]
>>> 19/07/12 11:02:02 INFO topicstress.KafkaGetAssigment$: Assigment: [ceb36686-49f2-4a8f-9956-fa833a5c019e-0]
>>> 19/07/12 11:02:03 INFO topicstress.KafkaGetAssigment$: Assigment: [ceb36686-49f2-4a8f-9956-fa833a5c019e-0]
```

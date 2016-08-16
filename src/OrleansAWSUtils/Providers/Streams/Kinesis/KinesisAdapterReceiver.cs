
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using OrleansAWSUtils;

namespace Orleans.Kinesis.Providers
{
    internal class KinesisPartitionConfig
    {
        public IKinesisSettings Hub { get; set; }
        public Shard Shard { get; set; }
    }


    internal class KinesisAdapterReceiver : IQueueAdapterReceiver, IQueueCache
    {
        public const int MaxMessagesPerRead = 1000;
        private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(5);

        private readonly KinesisPartitionConfig config;
        private readonly Func<Shard, IStreamQueueCheckpointer<string>, Logger, IKinesisQueueCache> cacheFactory;
        private readonly Func<Shard, Task<IStreamQueueCheckpointer<string>>> checkpointerFactory;
        private readonly Logger baseLogger;
        private readonly Logger logger;

        // metric names
        private readonly string hubReceiveTimeMetric;
        private readonly string partitionReceiveTimeMetric;
        private readonly string hubReadFailure;
        private readonly string partitionReadFailure;
        private readonly string hubMessagesRecieved;
        private readonly string partitionMessagesReceived;
        private readonly string hubAgeOfMessagesBeingProcessed;
        private readonly string partitionAgeOfMessagesBeingProcessed;

        private IKinesisQueueCache cache;
        private IAmazonKinesis client;
        private string shardIterator;

        //private KinesisReceiver receiver;
        private IStreamQueueCheckpointer<string> checkpointer;
        private AggregatedQueueFlowController flowController;

        // Receiver life cycle
        private int recieverState = ReceiverShutdown;
        private const int ReceiverShutdown = 0;
        private const int ReceiverRunning = 1;

        public int GetMaxAddCount() { return flowController.GetMaxAddCount(); }

        public KinesisAdapterReceiver(KinesisPartitionConfig partitionConfig,
            Func<Shard, IStreamQueueCheckpointer<string>, Logger,IKinesisQueueCache> cacheFactory,
            Func<Shard, Task<IStreamQueueCheckpointer<string>>> checkpointerFactory,
            Logger logger)
        {
            this.cacheFactory = cacheFactory;
            this.checkpointerFactory = checkpointerFactory;
            baseLogger = logger;
            this.logger = logger.GetSubLogger("-receiver");
            config = partitionConfig;

            client = config.Hub.KinesisConfig != null ? new AmazonKinesisClient(config.Hub.KinesisConfig) : new AmazonKinesisClient();

            hubReceiveTimeMetric = $"Orleans.Kinesis.ReceiveTime_{config.Hub.StreamName}";
            partitionReceiveTimeMetric = $"Orleans.Kinesis.ReceiveTime_{config.Hub.StreamName}-{config.Shard}";
            hubReadFailure = $"Orleans.Kinesis.ReadFailure_{config.Hub.StreamName}";
            partitionReadFailure = $"Orleans.Kinesis.ReadFailure_{config.Hub.StreamName}-{config.Shard}";
            hubMessagesRecieved = $"Orleans.Kinesis.MessagesReceived_{config.Hub.StreamName}";
            partitionMessagesReceived = $"Orleans.Kinesis.MessagesReceived_{config.Hub.StreamName}-{config.Shard}";
            hubAgeOfMessagesBeingProcessed = $"Orleans.Kinesis.AgeOfMessagesBeingProcessed_{config.Hub.StreamName}";
            partitionAgeOfMessagesBeingProcessed = $"Orleans.Kinesis.AgeOfMessagesBeingProcessed_{config.Hub.StreamName}-{config.Shard}";
        }

        public Task Initialize(TimeSpan timeout)
        {
            logger.Info("Initializing Kinesis partition {0}-{1}.", config.Hub.StreamName, config.Shard);
            // if receiver was already running, do nothing
            return ReceiverRunning == Interlocked.Exchange(ref recieverState, ReceiverRunning) ? TaskDone.Done : Initialize();
        }

        /// <summary>
        /// Initialization of Kinesis receiver is performed at adapter reciever initialization, but if it fails,
        ///  it will be retried when messages are requested
        /// </summary>
        /// <returns></returns>
        private async Task Initialize()
        {
            checkpointer = await checkpointerFactory(config.Shard);
            cache = cacheFactory(config.Shard, checkpointer, baseLogger);
            flowController = new AggregatedQueueFlowController(MaxMessagesPerRead) { cache };
            string offset = await checkpointer.Load();

            shardIterator = await CreateIterator(config, offset);
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            if (recieverState==ReceiverShutdown || maxCount <= 0)
            {
                return new List<IBatchContainer>();
            }

            // if receiver initialization failed, retry
            if (string.IsNullOrWhiteSpace(shardIterator))
            {
                logger.Warn(KinesisErrorCode.FailedPartitionRead, "Retrying initialization of Kinesis partition {0}-{1}.", config.Hub.StreamName, config.Shard);
                await Initialize();
                if (string.IsNullOrWhiteSpace(shardIterator))
                {
                    // should not get here, should throw instead, but just incase.
                    return new List<IBatchContainer>();
                }
            }

            List<Record> messages;
            try
            {
                var watch = Stopwatch.StartNew();
                var response =
                    await
                        client.GetRecordsAsync(new GetRecordsRequest {Limit = maxCount, ShardIterator = shardIterator});

                messages = response.Records;
                shardIterator = response.NextShardIterator;

                watch.Stop();

                logger.TrackMetric(hubReceiveTimeMetric, watch.Elapsed);
                logger.TrackMetric(partitionReceiveTimeMetric, watch.Elapsed);
                logger.TrackMetric(hubReadFailure, 0);
                logger.TrackMetric(partitionReadFailure, 0);
            }
            catch (Exception ex)
            {
                logger.TrackMetric(hubReadFailure, 1);
                logger.TrackMetric(partitionReadFailure, 1);
                logger.Warn(KinesisErrorCode.FailedPartitionRead, "Failed to read from Kinesis partition {0}-{1}. : Exception: {2}.", config.Hub.StreamName,
                    config.Shard, ex);
                throw;
            }

            var batches = new List<IBatchContainer>();
            if (messages.Count == 0)
            {
                return batches;
            }

            logger.TrackMetric(hubMessagesRecieved, messages.Count);
            logger.TrackMetric(partitionMessagesReceived, messages.Count);

            // monitor message age
            var dequeueTimeUtc = DateTime.UtcNow;
            TimeSpan difference = dequeueTimeUtc - messages[messages.Count-1].ApproximateArrivalTimestamp;
            logger.TrackMetric(hubAgeOfMessagesBeingProcessed, difference);
            logger.TrackMetric(partitionAgeOfMessagesBeingProcessed, difference);

            foreach (Record message in messages)
            {
                StreamPosition streamPosition = cache.Add(message, dequeueTimeUtc);
                batches.Add(new StreamActivityNotificationBatch(streamPosition.StreamIdentity.Guid,
                    streamPosition.StreamIdentity.Namespace, streamPosition.SequenceToken));
            }

            if (!checkpointer.CheckpointExists)
            {
                checkpointer.Update(messages[0].SequenceNumber, DateTime.UtcNow);
            }
            return batches;
        }

        public void AddToCache(IList<IBatchContainer> messages)
        {
            // do nothing, we add data directly into cache.  No need for agent involvement
        }

        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            purgedItems = null;
            return false;
        }

        public IQueueCacheCursor GetCacheCursor(IStreamIdentity streamIdentity, StreamSequenceToken token)
        {
            return new Cursor(cache, streamIdentity, token);
        }

        public bool IsUnderPressure()
        {
            return false;
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            return TaskDone.Done;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            // if receiver was already shutdown, do nothing
            if (ReceiverShutdown == Interlocked.Exchange(ref recieverState, ReceiverShutdown))
            {
                return TaskDone.Done;
            }

            logger.Info("Stopping reading from Kinesis partition {0}-{1}", config.Hub.StreamName, config.Shard);

            // clear cache and receiver
            IKinesisQueueCache localCache = Interlocked.Exchange(ref cache, null);
            IAmazonKinesis localReceiver = Interlocked.Exchange(ref client, null);
            // start closing receiver
            Task closeTask = TaskDone.Done;

            localReceiver?.Dispose();
            // dispose of cache
            localCache?.Dispose();
            // finish return receiver closing task
            return closeTask;
        }

        private async Task<string> CreateIterator(KinesisPartitionConfig partitionConfig, string offset)
        {
            var shardIteratorRequest = new GetShardIteratorRequest
            {               
                ShardId = partitionConfig.Shard.ShardId,
                StreamName = partitionConfig.Hub.StreamName,
            };


            //KinesisClient client = KinesisClient.CreateFromConnectionString(partitionConfig.Hub.KinesisConfig, partitionConfig.Hub.StreamName);
            //KinesisConsumerGroup consumerGroup = client.GetConsumerGroup(partitionConfig.Hub.ConsumerGroup);
            //// if we have a starting offset or if we're not configured to start reading from utc now, read from offset
            if (!partitionConfig.Hub.StartFromNow || offset != string.Empty)
            {
                shardIteratorRequest.ShardIteratorType =
                    KinesisPartitionCheckpointEntity.ITERATOR_TYPE_AFTER_SEQUENCE_NUMBER;
                shardIteratorRequest.StartingSequenceNumber = offset;
                logger.Info("Starting to read from Kinesis partition {0}-{1} at offset {2}", partitionConfig.Hub.StreamName, partitionConfig.Shard, offset);
            }
            else
            {
                shardIteratorRequest.ShardIteratorType = KinesisPartitionCheckpointEntity.ITERATOR_TYPE_LATEST;
                logger.Info("Starting to read latest messages from Kinesis partition {0}-{1} at offset {2}", partitionConfig.Hub.StreamName, partitionConfig.Shard, offset);
            }
            return (await client.GetShardIteratorAsync(shardIteratorRequest)).ShardIterator;
        }

        private class StreamActivityNotificationBatch : IBatchContainer
        {
            public Guid StreamGuid { get; }
            public string StreamNamespace { get; }
            public StreamSequenceToken SequenceToken { get; }

            public StreamActivityNotificationBatch(Guid streamGuid, string streamNamespace,
                StreamSequenceToken sequenceToken)
            {
                StreamGuid = streamGuid;
                StreamNamespace = streamNamespace;
                SequenceToken = sequenceToken;
            }

            public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() { throw new NotSupportedException(); }
            public bool ImportRequestContext() { throw new NotSupportedException(); }
            public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc) { throw new NotSupportedException(); }
        }

        private class Cursor : IQueueCacheCursor
        {
            private readonly IKinesisQueueCache cache;
            private readonly object cursor;
            private IBatchContainer current;

            public Cursor(IKinesisQueueCache cache, IStreamIdentity streamIdentity, StreamSequenceToken token)
            {
                this.cache = cache;
                cursor = cache.GetCursor(streamIdentity, token);
            }

            public void Dispose()
            {
            }

            public IBatchContainer GetCurrent(out Exception exception)
            {
                exception = null;
                return current;
            }

            public bool MoveNext()
            {
                IBatchContainer next;
                if (!cache.TryGetNextMessage(cursor, out next))
                {
                    return false;
                }

                current = next;
                return true;
            }

            public void Refresh()
            {
            }

            public void RecordDeliveryFailure()
            {
            }
        }
    }
}

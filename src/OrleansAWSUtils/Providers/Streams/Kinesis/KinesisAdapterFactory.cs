
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Util;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.ServiceBus.Providers;
using Orleans.Streams;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Queue adapter factory which allows the PersistentStreamProvider to use Kinesis as its backend persistent event queue.
    /// </summary>
    public class KinesisAdapterFactory : IQueueAdapterFactory, IQueueAdapter, IQueueAdapterCache
    {
        protected Logger logger;
        protected IServiceProvider serviceProvider;
        protected IProviderConfiguration providerConfig;
        protected KinesisStreamProviderConfig adapterConfig;
        protected IKinesisSettings kinesisSettings;
        protected ICheckpointerSettings checkpointerSettings;
        private IKinseisQueueMapper streamQueueMapper;
        private List<Shard> shards;
        private ConcurrentDictionary<QueueId, KinesisAdapterReceiver> receivers;
        private Amazon.Kinesis.AmazonKinesisClient client;
        private AmazonKinesisConfig clientConfig;

        /// <summary>
        /// Name of the adapter. Primarily for logging purposes
        /// </summary>
        public string Name => adapterConfig.StreamProviderName;

        /// <summary>
        /// Determines whether this is a rewindable stream adapter - supports subscribing from previous point in time.
        /// </summary>
        /// <returns>True if this is a rewindable stream adapter, false otherwise.</returns>
        public bool IsRewindable => true;

        /// <summary>
        /// Direction of this queue adapter: Read, Write or ReadWrite.
        /// </summary>
        /// <returns>The direction in which this adapter provides data.</returns>
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        /// <summary>
        /// Creates a message cache for an Kinesis shard.
        /// </summary>
        protected Func<Shard, IStreamQueueCheckpointer<string>, Logger, IKinesisQueueCache> CacheFactory { get; set; }
        /// <summary>
        /// Creates a shard checkpointer.
        /// </summary>
        protected Func<Shard, Task<IStreamQueueCheckpointer<string>>> CheckpointerFactory { get; set; }
        /// <summary>
        /// Creates a failure handler for a shard.
        /// </summary>
        protected Func<string, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { get; set; }
        /// <summary>
        /// Create a queue mapper to map Kinesis shards to queues
        /// </summary>
        protected Func<List<Shard>, IKinseisQueueMapper> QueueMapperFactory { get; set; }

        /// <summary>
        /// Factory initialization.
        /// Provider config must contain the kinesis settings type or the settings themselves.
        /// KinesisSettingsType is recommended for consumers that do not want to include secure information in the cluster configuration.
        /// </summary>
        /// <param name="providerCfg"></param>
        /// <param name="providerName"></param>
        /// <param name="log"></param>
        /// <param name="svcProvider"></param>
        public virtual void Init(IProviderConfiguration providerCfg, string providerName, Logger log, IServiceProvider svcProvider)
        {
            if (providerCfg == null) throw new ArgumentNullException("providerCfg");
            if (string.IsNullOrWhiteSpace(providerName)) throw new ArgumentNullException("providerName");
            if (log == null) throw new ArgumentNullException("log");
            //if (svcProvider == null) throw new ArgumentNullException("svcProvider");

            providerConfig = providerCfg;
            serviceProvider = svcProvider;
            receivers = new ConcurrentDictionary<QueueId, KinesisAdapterReceiver>();

            adapterConfig = new KinesisStreamProviderConfig(providerName);
            adapterConfig.PopulateFromProviderConfig(providerConfig);
            kinesisSettings = adapterConfig.GetKinesisSettings(providerConfig, serviceProvider);
            
            client = new AmazonKinesisClient(kinesisSettings.KinesisConfig); // KinesisClient.CreateFromConnectionString(kinesisSettings.KinesisConfig, kinesisSettings.StreamName);            

            if (CheckpointerFactory == null)
            {
                checkpointerSettings = adapterConfig.GetCheckpointerSettings(providerConfig, serviceProvider);
                CheckpointerFactory = partition => KinesisCheckpointer.Create(checkpointerSettings, adapterConfig.StreamProviderName, partition);
            }
            
            if (CacheFactory == null)
            {
                var bufferPool = new FixedSizeObjectPool<FixedSizeBuffer>(adapterConfig.CacheSizeMb, () => new FixedSizeBuffer(1 << 20));
                CacheFactory = (partition,checkpointer,cacheLogger) => new KinesisQueueCache(checkpointer, bufferPool, cacheLogger);
            }

            if (StreamFailureHandlerFactory == null)
            {
                //TODO: Add a queue specific default failure handler with reasonable error reporting.
                StreamFailureHandlerFactory = partition => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
            }

            if (QueueMapperFactory == null)
            {
                QueueMapperFactory = partitions => new KinesisQueueMapper(shards, adapterConfig.StreamProviderName);
            }

            logger = log.GetLogger($"Kinesis.{kinesisSettings.StreamName}");
        }

        /// <summary>
        /// Create queue adapter.
        /// </summary>
        /// <returns></returns>
        public async Task<IQueueAdapter> CreateAdapter()
        {
            if (streamQueueMapper == null)
            {
                shards = await GetStreamShardsAsync();
                streamQueueMapper = QueueMapperFactory(shards);
            }
            return this;
        }

        /// <summary>
        /// Create queue message cache adapter
        /// </summary>
        /// <returns></returns>
        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return this;
        }

        /// <summary>
        /// Create queue mapper
        /// </summary>
        /// <returns></returns>
        public IStreamQueueMapper GetStreamQueueMapper()
        {
            //TODO: CreateAdapter must be called first.  Figure out how to safely enforce this
            return streamQueueMapper;
        }

        /// <summary>
        /// Aquire delivery failure handler for a queue
        /// </summary>
        /// <param name="queueId"></param>
        /// <returns></returns>
        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return StreamFailureHandlerFactory(streamQueueMapper.QueueToShard(queueId).ShardId);
        }

        /// <summary>
        /// Writes a set of events to the queue as a single batch associated with the provided streamId.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="streamGuid"></param>
        /// <param name="streamNamespace"></param>
        /// <param name="events"></param>
        /// <param name="token"></param>
        /// <param name="requestContext"></param>
        /// <returns></returns>
        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events,
            StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (token != null)
            {
                throw new NotImplementedException(
                    "Kinesis stream provider currently does not support non-null StreamSequenceToken.");
            }
            return client.PutRecordsAsync(KinesisBatchContainer.ToPutRecordsRequest(kinesisSettings.StreamName, new StreamIdentity(streamGuid, streamNamespace), events, requestContext));

        }

        /// <summary>
        /// Creates a quere receiver for the specificed queueId
        /// </summary>
        /// <param name="queueId"></param>
        /// <returns></returns>
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return GetOrCreateReceiver(queueId);
        }

        /// <summary>
        /// Create a cache for a given queue id
        /// </summary>
        /// <param name="queueId"></param>
        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return GetOrCreateReceiver(queueId);
        }

        private KinesisAdapterReceiver GetOrCreateReceiver(QueueId queueId)
        {
            return receivers.GetOrAdd(queueId, q => MakeReceiver(queueId));
        }

        private KinesisAdapterReceiver MakeReceiver(QueueId queueId)
        {
            var config = new KinesisPartitionConfig
            {
                Hub = kinesisSettings,
                Shard = streamQueueMapper.QueueToShard(queueId),
            };
            Logger recieverLogger = logger.GetSubLogger($".{config.Shard}");
            return new KinesisAdapterReceiver(config, CacheFactory, CheckpointerFactory, recieverLogger);
        }

        private async Task<List<Shard>> GetStreamShardsAsync(string exclusiveStartShardId = "")
        {
            var request = new DescribeStreamRequest
            {
                Limit = 50,
                StreamName = kinesisSettings.StreamName
            };

            if (!string.IsNullOrWhiteSpace(exclusiveStartShardId))
            {
                request.ExclusiveStartShardId = exclusiveStartShardId;
            }

            var description = await client.DescribeStreamAsync(request);

            return description.StreamDescription.HasMoreShards
                ? description.StreamDescription.Shards.Concat(
                    await GetStreamShardsAsync(description.StreamDescription.Shards.Last().ShardId))
                    .ToList()
                : description.StreamDescription.Shards;

        }
    }
}

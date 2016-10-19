using System;
using System.Numerics;
using System.Text;
using Amazon.Kinesis.Model;
using Orleans.Kinesis.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.ServiceBus.Providers;
using Orleans.Streams;

namespace Tester.TestStreamProviders.Kinesis
{
    public class StreamPerPartitionKinesisStreamProvider : PersistentStreamProvider<StreamPerPartitionKinesisStreamProvider.AdapterFactory>
    {
        public class AdapterFactory : KinesisAdapterFactory
        {
            private TimePurgePredicate timePurgePredicate = null;

            public AdapterFactory()
            {
                CacheFactory = CreateQueueCache;
            }

            private IKinesisQueueCache CreateQueueCache(Shard shard, IStreamQueueCheckpointer<string> checkpointer, Logger log)
            {
                if (timePurgePredicate != null)
                {
                    timePurgePredicate = new TimePurgePredicate(adapterConfig.DataMinTimeInCache, adapterConfig.DataMaxAgeInCache);
                }
                var bufferPool = new FixedSizeObjectPool<FixedSizeBuffer>(adapterConfig.CacheSizeMb, () => new FixedSizeBuffer(1 << 20));
                var dataAdapter = new CachedDataAdapter(shard, bufferPool, timePurgePredicate);
                return new KinesisQueueCache(checkpointer, dataAdapter, log);
            }
        }

        private class CachedDataAdapter : KinesisDataAdapter
        {
            private readonly Guid partitionStreamGuid;

            public CachedDataAdapter(Shard shard, IObjectPool<FixedSizeBuffer> bufferPool, TimePurgePredicate timePurge)
                : base(shard.ShardId, bufferPool, timePurge)
            {
                partitionStreamGuid = GetPartitionGuid(_shardId);
            }

            public override StreamPosition GetStreamPosition(Record queueMessage)
            {
                IStreamIdentity streamIdentity = new StreamIdentity(partitionStreamGuid, null);
                StreamSequenceToken token = new KinesisStreamSequenceToken(_shardId, BigInteger.Parse(queueMessage.SequenceNumber));
                return new StreamPosition(streamIdentity, token);
            }
        }

        public static Guid GetPartitionGuid(string partition)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(partition);
            Array.Resize(ref bytes, 10);
            return new Guid(partition.GetHashCode(), bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9]);
        }
    }
}

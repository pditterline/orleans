
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Amazon.Kinesis.Model;
using Orleans.Streams;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Queue mapper that tracks which Kinesis shard was mapped to which queueId
    /// </summary>
    public class KinesisQueueMapper : HashRingBasedStreamQueueMapper, IKinseisQueueMapper
    {
        private readonly Dictionary<QueueId, Shard> partitionDictionary = new Dictionary<QueueId, Shard>();

        /// <summary>
        /// Queue mapper that tracks which Kinesis shard was mapped to which queueId
        /// </summary>
        /// <param name="shards">List of Kinesis shards</param>
        /// <param name="queueNamePrefix">Prefix for queueIds.  Must be unique per stream provider</param>
        public KinesisQueueMapper(List<Shard> shards, string queueNamePrefix)
            : base(shards.Count, queueNamePrefix)
        {
            QueueId[] queues = GetAllQueues().ToArray();
            if (queues.Length != shards.Count)
            {
                throw new ArgumentOutOfRangeException("shards", "shards and Queues do not line up");
            }
            for (int i = 0; i < queues.Length; i++)
            {
                partitionDictionary.Add(queues[i], shards[i]);
            }
        }

        /// <summary>
        /// Gets the Kinesis shard by QueueId
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        public Shard QueueToShard(QueueId queue)
        {
            if (queue == null)
            {
                throw new ArgumentNullException("queue");
            }

            Shard shard;
            if (!partitionDictionary.TryGetValue(queue, out shard))
            {
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture, "queue {0}", queue.ToStringWithHashCode()));
            }
            return shard;
        }
    }
}

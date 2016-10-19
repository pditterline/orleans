using Amazon.Kinesis.Model;
using Orleans.Streams;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Stream queue mapper that maps Kinesis partitions to <see cref="QueueId"/>s
    /// </summary>
    public interface IKinseisQueueMapper : IStreamQueueMapper
    {
        /// <summary>
        /// Get a shard given a queue id.
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        Shard QueueToShard(QueueId queue);
    }
}

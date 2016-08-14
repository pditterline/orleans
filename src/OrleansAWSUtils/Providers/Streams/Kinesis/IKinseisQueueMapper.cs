using Amazon.Kinesis.Model;
using Orleans.Streams;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Stream queue mapper that maps Kinesis partitions to <see cref="QueueId"/>s
    /// </summary>
    public interface IKinseisQueueMapper : IStreamQueueMapper
    {
        Shard QueueToShard(QueueId queue);
    }
}

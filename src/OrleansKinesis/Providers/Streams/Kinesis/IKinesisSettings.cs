

namespace Orleans.Kinesis.Providers
{
    public interface IKinesisSettings
    {
        string ConnectionString { get; }
        string ConsumerGroup { get; }
        string StreamName { get; }
        
        /// <summary>
        /// Indicates if stream provider should read all new data in partition, or from start of partition.
        /// True - read all new data added to partition.
        /// False - start reading from beginning of partition.
        /// Note: If checkpoints are used, stream provider will always begin reading from most recent checkpoint.
        /// </summary>
        bool StartFromNow { get; }
    }
}

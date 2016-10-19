

using Amazon.Kinesis;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Interface for Kinesis provider settings.
    /// </summary>
    public interface IKinesisSettings
    {
        /// <summary>
        /// Kinesis configuration.
        /// </summary>
        AmazonKinesisConfig KinesisConfig { get; }
        /// <summary>
        /// Kinesis stream name.
        /// </summary>
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

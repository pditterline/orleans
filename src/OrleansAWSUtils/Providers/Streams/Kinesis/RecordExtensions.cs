
using System;
using System.Linq;
using Amazon.Kinesis.Model;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Extends Kinesis Records to support streaming
    /// </summary>
    public static class RecordExtensions
    {
        /// <summary>
        /// Get a Guid from this Kinesis Record.
        /// </summary>
        /// <param name="kinesisRecord"></param>
        /// <returns></returns>
        public static Guid GetStreamGuidProperty(this Record kinesisRecord)
        {
            return !kinesisRecord.PartitionKey.Contains('.')
                ? new Guid(kinesisRecord.PartitionKey)
                : new Guid(kinesisRecord.PartitionKey.Split('.').Last());
        }

        /// <summary>
        /// Get a stream namespace from this Kinesis Record.
        /// </summary>
        /// <param name="kinesisRecord"></param>
        /// <returns></returns>
        public static string GetStreamNamespaceProperty(this Record kinesisRecord)
        {
            return !kinesisRecord.PartitionKey.Contains('.')
                ? string.Empty
                : kinesisRecord.PartitionKey.Split('.').First();
        }

        /// <summary>
        /// Add a stream namespace to this Kinesis record.
        /// </summary>
        /// <param name="kinesisRecord"></param>
        /// <param name="streamNamespace"></param>
        public static void SetStreamNamespaceProperty(this Record kinesisRecord, string streamNamespace)
        {
            kinesisRecord.PartitionKey = $"{streamNamespace}.{kinesisRecord.PartitionKey}";
        }
    }
}

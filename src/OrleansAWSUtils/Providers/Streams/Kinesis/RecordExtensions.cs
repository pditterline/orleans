
using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.Kinesis.Model;
using Orleans.Serialization;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Extends EventData to support streaming
    /// </summary>
    public static class RecordExtensions
    {
        public static Guid GetStreamGuidProperty(this Record kinesisRecord)
        {
            return !kinesisRecord.PartitionKey.Contains('.')
                ? new Guid(kinesisRecord.PartitionKey)
                : new Guid(kinesisRecord.PartitionKey.Split('.').Last());
        }


        public static string GetStreamNamespaceProperty(this Record kinesisRecord)
        {
            return !kinesisRecord.PartitionKey.Contains('.')
                ? string.Empty
                : kinesisRecord.PartitionKey.Split('.').First();
        }

        public static void SetStreamNamespaceProperty(this Record kinesisRecord, string streamNamespace)
        {
            kinesisRecord.PartitionKey = $"{streamNamespace}.{kinesisRecord.PartitionKey}";
        }
    }
}

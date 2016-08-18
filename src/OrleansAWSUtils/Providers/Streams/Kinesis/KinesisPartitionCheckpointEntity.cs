
using System;
using Amazon.Kinesis.Model;

namespace Orleans.Kinesis.Providers
{
    internal class KinesisPartitionCheckpointEntity
    {
        public const string ITERATOR_TYPE_TRIM_HORIZON = "TRIM_HORIZON";
        public const string ITERATOR_TYPE_AT_SEQUENCE_NUMBER = "AT_SEQUENCE_NUMBER";
        public const string ITERATOR_TYPE_AFTER_SEQUENCE_NUMBER = "AFTER_SEQUENCE_NUMBER";
        public const string ITERATOR_TYPE_LATEST = "LATEST";
        public const string ITERATOR_TYPE_AT_TIMESTAMP = "AT_TIMESTAMP";

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Offset { get; set; }

        public KinesisPartitionCheckpointEntity()
        {
            Offset = String.Empty;
        }

        public static KinesisPartitionCheckpointEntity Create(string streamProviderName, string checkpointNamespace, Shard shard)
        {
            return new KinesisPartitionCheckpointEntity
            {
                PartitionKey = MakePartitionKey(streamProviderName, checkpointNamespace),
                RowKey = MakeRowKey(shard)
            };
        }

        public static string MakePartitionKey(string streamProviderName, string checkpointNamespace)
        {
            string key = $"KinesisCheckpoints_{streamProviderName}_{checkpointNamespace}";            
            return key;
        }

        public static string MakeRowKey(Shard partition)
        {
            string key = $"partition_{partition}";            
            return key;
            
        }
    }
}


using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Orleans.Streams;
using OrleansAWSUtils.Storage;
using Shard = Amazon.Kinesis.Model.Shard;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// This class stores Kinesis shard checkpointer information (a shard offset) in a DynamoDb Table.
    /// </summary>
    public class KinesisCheckpointer : IStreamQueueCheckpointer<string>
    {
        private readonly ICheckpointerSettings _settings;
        private readonly DynamoDBStorage _storage;
        private readonly TimeSpan persistInterval;        
        private KinesisPartitionCheckpointEntity entity;
        private Task inProgressSave;
        private DateTime? throttleSavesUntilUtc;

        /// <summary>
        /// Indicates if a checkpoint exists
        /// </summary>
        public bool CheckpointExists => entity != null && entity.Offset != String.Empty;

        /// <summary>
        /// Factory function that creates and initializes the checkpointer
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="streamProviderName"></param>
        /// <param name="shard"></param>
        /// <returns></returns>
        public static async Task<IStreamQueueCheckpointer<string>> Create(ICheckpointerSettings settings, string streamProviderName, Shard shard)
        {
            var checkpointer = new KinesisCheckpointer(settings, streamProviderName, shard);
            await checkpointer.Initialize();
            return checkpointer;
        }

        private KinesisCheckpointer(ICheckpointerSettings settings, string streamProviderName, Shard shard)
        {
            _settings = settings;
            if (settings == null)
            {
                throw new ArgumentNullException("settings");
            }
            if (string.IsNullOrWhiteSpace(streamProviderName))
            {
                throw new ArgumentNullException("streamProviderName");
            }
            if (shard == null)
            {
                throw new ArgumentNullException("shard");
            }            

            _storage = new DynamoDBStorage(settings.DataConnectionString);

            persistInterval = settings.PersistInterval;
                        
            entity = KinesisPartitionCheckpointEntity.Create(streamProviderName, settings.CheckpointNamespace, shard);
        }

        private async Task Initialize()
        {
            await
                _storage.InitializeTable(_settings.TableName,
                    new List<KeySchemaElement>
                    {
                        new KeySchemaElement("PartitionKey", KeyType.HASH),
                        new KeySchemaElement("RowKey", KeyType.RANGE)
                    },
                    new List<AttributeDefinition>
                    {
                        new AttributeDefinition("PartitionKey", ScalarAttributeType.S),
                        new AttributeDefinition("RowKey", ScalarAttributeType.S),
                    });
        }

        /// <summary>
        /// Loads a checkpoint
        /// </summary>
        /// <returns></returns>
        public async Task<string> Load()
        {
            var result = await _storage.ReadSingleEntryAsync(_settings.TableName,
                        new Dictionary<string, AttributeValue>
                        {
                            {"PartitionKey", new AttributeValue(entity.PartitionKey)},
                            {"RowKey", new AttributeValue(entity.RowKey)}
                        },
                        item => item.ToCheckpointEntity()
                        );
            if (result != null)
            {
                entity = result;
            }
            return entity.Offset;
        }

        /// <summary>
        /// Updates the checkpoint.  This is a best effort.  It does not always update the checkpoint.
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="utcNow"></param>
        public void Update(string offset, DateTime utcNow)
        {
            // if offset has not changed, do nothing
            if (string.Compare(entity.Offset, offset, StringComparison.InvariantCulture)==0)
            {
                return;
            }

            // if we've saved before but it's not time for another save or the last save operation has not completed, do nothing
            if (throttleSavesUntilUtc.HasValue && (throttleSavesUntilUtc.Value > utcNow || !inProgressSave.IsCompleted))
            {
                return;
            }

            entity.Offset = offset;
            throttleSavesUntilUtc = utcNow + persistInterval;
            inProgressSave = _storage.UpsertEntryAsync(
                _settings.TableName,
                new Dictionary<string, AttributeValue>()
                {
                    {"PartitionKey", new AttributeValue {S = entity.PartitionKey}},
                    {"RowKey", new AttributeValue {S = entity.RowKey}},
                },
                new Dictionary<string, AttributeValue>()
                {
                    {"StreamOffset", new AttributeValue(entity.Offset) }
                });
            inProgressSave.Ignore();
        }
    }

    internal static class CheckpointEntityHelper
    {
        public static KinesisPartitionCheckpointEntity ToCheckpointEntity(
            this Dictionary<string, AttributeValue> entityData)
        {
            return new KinesisPartitionCheckpointEntity
            {
                Offset = entityData["StreamOffset"].S,
                PartitionKey = entityData["PartitionKey"].S,
                RowKey = entityData["RowKey"].S,
            };
        }
    }
}

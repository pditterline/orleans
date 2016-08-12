
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Orleans.Streams;
using Shard = Amazon.Kinesis.Model.Shard;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// This class stores Kinesis shard checkpointer information (a shard offset) in a DynamoDb Table.
    /// </summary>
    public class KinesisCheckpointer : IStreamQueueCheckpointer<string>
    {
        private readonly ICheckpointerSettings _settings;
        private readonly IAmazonDynamoDB dataManager;
        private readonly TimeSpan persistInterval;

        private KinesisPartitionCheckpointEntity entity;
        private Task inProgressSave;
        private DateTime? throttleSavesUntilUtc;

        public bool CheckpointExists => entity != null && entity.Offset != KinesisPartitionCheckpointEntity.ITERATOR_TYPE_TRIM_HORIZON;

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
            persistInterval = settings.PersistInterval;
            dataManager = new AmazonDynamoDBClient();            

            //AzureTableDataManager<KinesisPartitionCheckpointEntity>(settings.TableName, settings.DataConnectionString);
            entity = KinesisPartitionCheckpointEntity.Create(streamProviderName, settings.CheckpointNamespace, shard);
        }

        private async Task Initialize()
        {
            try
            {
                var table = await dataManager.DescribeTableAsync(_settings.TableName);
                if (table == null)
                {
                    await CreateTable();
                }
            }
            catch (ResourceNotFoundException)
            {
                await CreateTable();
            }
        }

        private Task CreateTable()
        {
            return dataManager.CreateTableAsync(new CreateTableRequest(_settings.TableName,
                        new List<KeySchemaElement>
                        {
                            new KeySchemaElement("PartitionKey", KeyType.HASH),
                            new KeySchemaElement("RowKey", KeyType.RANGE)
                        }));
        }

        public async Task<string> Load()
        {
            var results = 
                await dataManager.GetItemAsync(_settings.TableName,
                        new Dictionary<string, AttributeValue>
                        {
                            {"PartitionKey", new AttributeValue(entity.PartitionKey)},
                            {"RowKey", new AttributeValue(entity.RowKey)}
                        });

            if (results != null)
            {
                entity = results.Item.ToCheckpointEntity();
            }
            return entity.Offset;
        }

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
            inProgressSave = dataManager.UpdateItemAsync(entity.ToUpdateRequest(_settings.TableName));
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
                Offset = entityData["Offset"].S,
                PartitionKey = entityData["PartitionKey"].S,
                RowKey = entityData["RowKey"].S,
            };
        }

        public static UpdateItemRequest ToUpdateRequest(this KinesisPartitionCheckpointEntity entity, string tableName)
        {
            return new UpdateItemRequest
            {
                Key = new Dictionary<string, AttributeValue>()
                {
                  { "PartitionKey", new AttributeValue { S = entity.PartitionKey } },
                  { "RowKey", new AttributeValue { S = entity.RowKey} },
                },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
                {
                    { "Offset", new AttributeValueUpdate(new AttributeValue { S = entity.Offset}, AttributeAction.PUT ) },
                },
                TableName = tableName
            };
        }
    }
}

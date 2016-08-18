using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureUtils;
using Orleans.Streams;
using Orleans.TestingHost;
using OrleansAWSUtils.Providers.Streams.PersistentStreams;
using OrleansAWSUtils.Storage;

namespace Tester.TestStreamProviders
{
    public class TestDynamoDbStorageStreamFailureHandler : DynamoDbStorageStreamFailureHandler<StreamDeliveryFailureEntity>
    {
        private const string TableName = "TestStreamFailures";
        private const string DeploymentId = "TestDeployment";

        private TestDynamoDbStorageStreamFailureHandler()
            : base(false, DeploymentId, TableName, StorageTestConstants.DynamoDBConnectionString)
        {
        }

        public static async Task<IStreamFailureHandler> Create()
        {
            var failureHandler = new TestDynamoDbStorageStreamFailureHandler();
            await failureHandler.InitAsync();
            return failureHandler;
        }

        public static async Task<int> GetDeliveryFailureCount(string streamProviderName)
        {
            var dataManager = new DynamoDBStorage(StorageTestConstants.DynamoDBConnectionString);
                            
            dataManager.InitializeTable(TableName, new List<KeySchemaElement>
            {
                new KeySchemaElement("PartitionKey", KeyType.HASH),
                new KeySchemaElement("RowKey", KeyType.RANGE)
            },
            new List<AttributeDefinition>
            {
                new AttributeDefinition("PartitionKey", ScalarAttributeType.S),
                new AttributeDefinition("RowKey", ScalarAttributeType.S),
            }).Wait();
            IEnumerable<StreamDeliveryFailureEntity> deliveryErrors =
                await
                    dataManager.QueryAsync(TableName, new Dictionary<string, AttributeValue>
                    {
                        {
                            "PartitionKey",
                            new AttributeValue(StreamDeliveryFailureEntity.MakeDefaultPartitionKey(streamProviderName,
                                DeploymentId))
                        },

                    }, "", StreamDeliveryFailureEntity.FromAttributeValueDictionary);

            return deliveryErrors.Count();
        }

        public static async Task DeleteAll()
        {
            var dataManager = new DynamoDBStorage(StorageTestConstants.DynamoDBConnectionString); //new AzureTableDataManager<TableEntity>(TableName, StorageTestConstants.DataConnectionString);
            await dataManager.InitializeTable(TableName, new List<KeySchemaElement>
            {
                new KeySchemaElement("PartitionKey", KeyType.HASH),
                new KeySchemaElement("RowKey", KeyType.RANGE)
            },
            new List<AttributeDefinition>
            {
                new AttributeDefinition("PartitionKey", ScalarAttributeType.S),
                new AttributeDefinition("RowKey", ScalarAttributeType.S),
            });
            await dataManager.DeleTableAsync(TableName);
        }
    }
}
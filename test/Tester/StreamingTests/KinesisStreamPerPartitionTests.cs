using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans;
using Orleans.AzureUtils;
using Orleans.Kinesis.Providers;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.ServiceBus.Providers;
using Orleans.Streams;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
using OrleansAWSUtils.Storage;
using Tester;
using Tester.TestStreamProviders.EventHub;
using Tester.TestStreamProviders.Kinesis;
using UnitTests.GrainInterfaces;
using UnitTests.Tester;
using Xunit;

namespace UnitTests.StreamingTests
{
    public class KinesisStreamPerPartitionTests : OrleansTestingBase, IClassFixture<KinesisStreamPerPartitionTests.Fixture>
    {
        private const string StreamProviderName = "KinesisStreamPerPartition";        
        private const string KinesisStream = "kinesisorleanstest";
        private const string KinesisCheckpointTable = "kinesischeckpoint";
        private static readonly string CheckpointNamespace = Guid.NewGuid().ToString();


        private static readonly KinesisSettings KinesisConfig = new KinesisSettings(StorageTestConstants.KinesisConnectionString, KinesisStream);

        private static readonly EventHubStreamProviderConfig ProviderConfig =
            new EventHubStreamProviderConfig(StreamProviderName);

        private static readonly KinesisCheckpointerSettings CheckpointerSettings =
            new KinesisCheckpointerSettings(StorageTestConstants.DynamoDBConnectionString, KinesisCheckpointTable, CheckpointNamespace, TimeSpan.FromSeconds(1));

        private class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions(2);
                // register stream provider
                options.ClusterConfiguration.AddMemoryStorageProvider("PubSubStore");
                options.ClusterConfiguration.Globals.RegisterStreamProvider<StreamPerPartitionKinesisStreamProvider>(StreamProviderName, BuildProviderSettings());
                options.ClientConfiguration.RegisterStreamProvider<KinesisStreamProvider>(StreamProviderName, BuildProviderSettings());

                // While we're building the cluster we should create the stream... not sure where else I can set up the stream in the Kinesis client so far... in production the stream would already exist... for test we need to create and destroy.
                var kinesisClient = new AmazonKinesisClient(KinesisConfig.KinesisConfig);
                kinesisClient.CreateStreamAsync(new CreateStreamRequest
                {
                    ShardCount = 2,
                    StreamName = KinesisStream,
                }).Wait();

                return new TestCluster(options);
            }

            public override void Dispose()
            {
                base.Dispose();
                var dataManager = new DynamoDBStorage(CheckpointerSettings.DataConnectionString); // new AzureTableDataManager<TableEntity>(CheckpointerSettings.TableName, CheckpointerSettings.DataConnectionString);
                dataManager.InitializeTable(CheckpointerSettings.TableName, new List<KeySchemaElement>
                {
                    new KeySchemaElement("PartitionKey", KeyType.HASH),
                    new KeySchemaElement("RowKey", KeyType.RANGE)
                },
                    new List<AttributeDefinition>
                    {
                        new AttributeDefinition("PartitionKey", ScalarAttributeType.S),
                        new AttributeDefinition("RowKey", ScalarAttributeType.S),
                    }).Wait();

                var kinesisClient = new AmazonKinesisClient(KinesisConfig.KinesisConfig);
                kinesisClient.DeleteStreamAsync(new DeleteStreamRequest
                {
                    StreamName = KinesisStream,
                }).Wait();


                dataManager.ClearTableAsync(CheckpointerSettings.TableName).Wait();
            }

            private static Dictionary<string, string> BuildProviderSettings()
            {
                var settings = new Dictionary<string, string>();

                // get initial settings from configs
                ProviderConfig.WriteProperties(settings);
                KinesisConfig.WriteProperties(settings);
                CheckpointerSettings.WriteProperties(settings);

                // add queue balancer setting
                settings.Add(PersistentStreamProviderConfig.QUEUE_BALANCER_TYPE, StreamQueueBalancerType.StaticClusterConfigDeploymentBalancer.ToString());

                return settings;
            }
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task Kinesis100StreamsTo4PartitionStreamsTest()
        {
            logger.Info("************************ Kinesis100StreamsTo4PartitionStreamsTest *********************************");

            int streamCount = 100;
            int eventsInStream = 10;
            int partitionCount = 4;

            List<ISampleStreaming_ConsumerGrain> consumers = new List<ISampleStreaming_ConsumerGrain>(partitionCount);
            for (int i = 0; i < partitionCount; i++)
            {
                consumers.Add(GrainClient.GrainFactory.GetGrain<ISampleStreaming_ConsumerGrain>(Guid.NewGuid()));
            }

            // subscribe to each partition
            List<Task> becomeConsumersTasks = consumers
                .Select((consumer, i) => consumer.BecomeConsumer(StreamPerPartitionEventHubStreamProvider.GetPartitionGuid(i.ToString()), null, StreamProviderName))
                .ToList();
            await Task.WhenAll(becomeConsumersTasks);

            await GenerateEvents(streamCount, eventsInStream);
            await TestingUtils.WaitUntilAsync(assertIsTrue => CheckCounters(consumers, streamCount * eventsInStream, assertIsTrue), TimeSpan.FromSeconds(30));
        }

        private async Task GenerateEvents(int streamCount, int eventsInStream)
        {
            IStreamProvider streamProvider = GrainClient.GetStreamProvider(StreamProviderName);
            IAsyncStream<int>[] producers =
                Enumerable.Range(0, streamCount)
                    .Select(i => streamProvider.GetStream<int>(Guid.NewGuid(), null))
                    .ToArray();

            for (int i = 0; i < eventsInStream; i++)
            {
                // send event on each stream
                for (int j = 0; j < streamCount; j++)
                {
                    await producers[j].OnNextAsync(i);
                }
            }
        }

        private async Task<bool> CheckCounters(List<ISampleStreaming_ConsumerGrain> consumers, int totalEventCount, bool assertIsTrue)
        {
            List<Task<int>> becomeConsumersTasks = consumers
                .Select((consumer, i) => consumer.GetNumberConsumed())
                .ToList();
            int[] counts = await Task.WhenAll(becomeConsumersTasks);

            if (assertIsTrue)
            {
                // one stream per queue
                Assert.Equal(totalEventCount, counts.Sum());
            }
            else if (totalEventCount != counts.Sum())
            {
                return false;
            }
            return true;
        }
    }
}
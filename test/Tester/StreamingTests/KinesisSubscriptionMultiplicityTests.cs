using System;
using System.Collections.Generic;
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
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;
using OrleansAWSUtils.Storage;
using UnitTests.StreamingTests;
using UnitTests.Tester;
using Xunit;

namespace Tester.StreamingTests
{
    public class KinesisSubscriptionMultiplicityTests : OrleansTestingBase, IClassFixture<KinesisSubscriptionMultiplicityTests.Fixture>
    {
        private const string StreamProviderName = "KinesisStreamProvider";
        private const string StreamNamespace = "StreamNamespace";
        private const string KinesisStream = "kinesisorleanstest";
        private const string KinesisCheckpointTable = "kinesischeckpoint";        
        private static readonly string CheckpointNamespace = Guid.NewGuid().ToString();

        public static readonly KinesisStreamProviderConfig ProviderConfig = new KinesisStreamProviderConfig(StreamProviderName);

        private static readonly KinesisSettings KinesisConfig = new KinesisSettings(StorageTestConstants.KinesisConnectionString, KinesisStream);

        private static readonly KinesisCheckpointerSettings CheckpointerSettings = new KinesisCheckpointerSettings(StorageTestConstants.DynamoDBConnectionString, KinesisCheckpointTable, CheckpointNamespace, TimeSpan.FromSeconds(1));

        private readonly SubscriptionMultiplicityTestRunner runner;

        private class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions(2);
                AdjustClusterConfiguration(options.ClusterConfiguration);

                return new TestCluster(options);
            }

            public override void Dispose()
            {
                base.Dispose();
                var dataManager = new DynamoDBStorage(StorageTestConstants.DynamoDBConnectionString);
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
                dataManager.ClearTableAsync(CheckpointerSettings.TableName).Wait();
            }

            private static void AdjustClusterConfiguration(ClusterConfiguration config)
            {
                var settings = new Dictionary<string, string>();
                // get initial settings from configs
                ProviderConfig.WriteProperties(settings);
                KinesisConfig.WriteProperties(settings);
                CheckpointerSettings.WriteProperties(settings);

                // add queue balancer setting
                settings.Add(PersistentStreamProviderConfig.QUEUE_BALANCER_TYPE, StreamQueueBalancerType.DynamicClusterConfigDeploymentBalancer.ToString());

                // register stream provider
                config.Globals.RegisterStreamProvider<KinesisStreamProvider>(StreamProviderName, settings);
                config.Globals.RegisterStorageProvider<MemoryStorage>("PubSubStore");
            }
        }

        public KinesisSubscriptionMultiplicityTests()
        {
            runner = new SubscriptionMultiplicityTestRunner(StreamProviderName, GrainClient.Logger);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisMultipleParallelSubscriptionTest()
        {
            logger.Info("************************ MultipleParallelSubscriptionTest *********************************");
            await runner.MultipleParallelSubscriptionTest(Guid.NewGuid(), StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisMultipleLinearSubscriptionTest()
        {
            logger.Info("************************ MultipleLinearSubscriptionTest *********************************");
            await runner.MultipleLinearSubscriptionTest(Guid.NewGuid(), StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisMultipleSubscriptionTest_AddRemove()
        {
            logger.Info("************************ MultipleSubscriptionTest_AddRemove *********************************");
            await runner.MultipleSubscriptionTest_AddRemove(Guid.NewGuid(), StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisResubscriptionTest()
        {
            logger.Info("************************ ResubscriptionTest *********************************");
            await runner.ResubscriptionTest(Guid.NewGuid(), StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisResubscriptionAfterDeactivationTest()
        {
            logger.Info("************************ ResubscriptionAfterDeactivationTest *********************************");
            await runner.ResubscriptionAfterDeactivationTest(Guid.NewGuid(), StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisActiveSubscriptionTest()
        {
            logger.Info("************************ ActiveSubscriptionTest *********************************");
            await runner.ActiveSubscriptionTest(Guid.NewGuid(), StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisTwoIntermitentStreamTest()
        {
            logger.Info("************************ TwoIntermitentStreamTest *********************************");
            await runner.TwoIntermitentStreamTest(Guid.NewGuid());
        }
    }
}
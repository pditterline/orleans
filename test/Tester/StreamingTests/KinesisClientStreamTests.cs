using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureUtils;
using Orleans.Kinesis.Providers;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Tester.TestStreamProviders;
using Tester.TestStreamProviders.Kinesis;
using UnitTests.Grains;
using UnitTests.Tester;
using Xunit;
using Xunit.Abstractions;

namespace Tester.StreamingTests
{
    public class KinesisClientStreamTests : TestClusterPerTest
    {
        private const string StreamProviderName = "KinesisStreamProvider";
        private const string StreamNamespace = "StreamNamespace";
        private const string KinesisStream = "kinesisorleanstest";
        private const string KinesisCheckpointTable = "kinesischeckpoint";
        private static readonly string CheckpointNamespace = Guid.NewGuid().ToString();

        private static readonly KinesisSettings KinesisConfig = new KinesisSettings(StorageTestConstants.KinesisConnectionString, KinesisStream);

        private static readonly KinesisStreamProviderConfig ProviderConfig = new KinesisStreamProviderConfig(StreamProviderName, 3);

        private static readonly KinesisCheckpointerSettings CheckpointerSettings = new KinesisCheckpointerSettings(StorageTestConstants.DataConnectionString, KinesisCheckpointTable,
            CheckpointNamespace,
            TimeSpan.FromSeconds(10));

        private readonly ITestOutputHelper output;
        private readonly ClientStreamTestRunner runner;

        public KinesisClientStreamTests(ITestOutputHelper output)
        {
            this.output = output;
            runner = new ClientStreamTestRunner(this.HostedCluster);
        }

        public override TestCluster CreateTestCluster()
        {
            var options = new TestClusterOptions(2);
            AdjustConfig(options.ClusterConfiguration);
            AdjustConfig(options.ClientConfiguration);
            return new TestCluster(options);
        }

        public override void Dispose()
        {
            base.Dispose();
            var dataManager = new AzureTableDataManager<TableEntity>(CheckpointerSettings.TableName, CheckpointerSettings.DataConnectionString);
            dataManager.InitTableAsync().Wait();
            dataManager.ClearTableAsync().Wait();
            TestAzureTableStorageStreamFailureHandler.DeleteAll().Wait();
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisStreamProducerOnDroppedClientTest()
        {
            logger.Info("************************ EHStreamProducerOnDroppedClientTest *********************************");
            await runner.StreamProducerOnDroppedClientTest(StreamProviderName, StreamNamespace);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task KinesisStreamConsumerOnDroppedClientTest()
        {
            logger.Info("************************ EHStreamConsumerOnDroppedClientTest *********************************");
            await runner.StreamConsumerOnDroppedClientTest(StreamProviderName, StreamNamespace, output,
                () => TestAzureTableStorageStreamFailureHandler.GetDeliveryFailureCount(StreamProviderName), true);
        }

        private static void AdjustConfig(ClusterConfiguration config)
        {
            // register stream provider
            config.AddMemoryStorageProvider("PubSubStore");
            config.Globals.RegisterStreamProvider<TestKinesisStreamProvider>(StreamProviderName, BuildProviderSettings());
            config.Globals.ClientDropTimeout = TimeSpan.FromSeconds(5);
        }

        private static void AdjustConfig(ClientConfiguration config)
        {
            config.RegisterStreamProvider<KinesisStreamProvider>(StreamProviderName, BuildProviderSettings());
        }

        private static Dictionary<string, string> BuildProviderSettings()
        {
            var settings = new Dictionary<string, string>();
            // get initial settings from configs
            ProviderConfig.WriteProperties(settings);
            KinesisConfig.WriteProperties(settings);
            CheckpointerSettings.WriteProperties(settings);
            return settings;
        }
    }
}
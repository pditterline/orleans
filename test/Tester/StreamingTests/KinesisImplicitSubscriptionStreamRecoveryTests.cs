using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Orleans;
using Orleans.Kinesis.Providers;
using Orleans.Providers.Streams.Generator;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Streams;
using Orleans.TestingHost;
using OrleansAWSUtils.Storage;
using TestGrains;
using UnitTests.Tester;
using Xunit;

namespace Tester.StreamingTests
{
    public class KinesisImplicitSubscriptionStreamRecoveryTests : OrleansTestingBase, IClassFixture<Tester.StreamingTests.KinesisImplicitSubscriptionStreamRecoveryTests.Fixture>
    {
        private const string StreamProviderName = "KinesisStreamProvider";
        private const string StreamNamespace = "StreamNamespace";
        private const string KinesisStream = "kinesisorleanstest";
        private const string KinesisCheckpointTable = "kinesischeckpoint";
        private static readonly string CheckpointNamespace = Guid.NewGuid().ToString();

        private static readonly KinesisSettings KinesisConfig = new KinesisSettings(StorageTestConstants.KinesisConnectionString, KinesisStream);

        private static readonly KinesisStreamProviderConfig ProviderConfig = new KinesisStreamProviderConfig(StreamProviderName);

        private static readonly KinesisCheckpointerSettings CheckpointerSettings =
            new KinesisCheckpointerSettings(StorageTestConstants.DynamoDBConnectionString, KinesisCheckpointTable, CheckpointNamespace, TimeSpan.FromSeconds(1));

        private readonly ImplicitSubscritionRecoverableStreamTestRunner runner;

        private class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions(2);
                // register stream provider
                options.ClusterConfiguration.AddMemoryStorageProvider("Default");
                options.ClusterConfiguration.Globals.RegisterStreamProvider<KinesisStreamProvider>(StreamProviderName, BuildProviderSettings());
                options.ClientConfiguration.RegisterStreamProvider<KinesisStreamProvider>(StreamProviderName, BuildProviderSettings());

                return new TestCluster(options);
            }

            public override void Dispose()
            {
                base.Dispose();
                var dataManager = new DynamoDBStorage(CheckpointerSettings.DataConnectionString);
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

            private static Dictionary<string, string> BuildProviderSettings()
            {
                var settings = new Dictionary<string, string>();

                // get initial settings from configs
                ProviderConfig.WriteProperties(settings);
                KinesisConfig.WriteProperties(settings);
                CheckpointerSettings.WriteProperties(settings);

                // add queue balancer setting
                settings.Add(PersistentStreamProviderConfig.QUEUE_BALANCER_TYPE, StreamQueueBalancerType.DynamicClusterConfigDeploymentBalancer.ToString());

                // add pub/sub settting
                settings.Add(PersistentStreamProviderConfig.STREAM_PUBSUB_TYPE, StreamPubSubType.ImplicitOnly.ToString());
                return settings;
            }
        }

        public KinesisImplicitSubscriptionStreamRecoveryTests()
        {
            runner = new ImplicitSubscritionRecoverableStreamTestRunner(GrainClient.GrainFactory, StreamProviderName);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task Recoverable100EventStreamsWithTransientErrorsTest()
        {
            logger.Info("************************ Recoverable100EventStreamsWithTransientErrorsTest *********************************");
            await runner.Recoverable100EventStreamsWithTransientErrors(GenerateEvents, ImplicitSubscription_TransientError_RecoverableStream_CollectorGrain.StreamNamespace, 4, 100);
        }

        [Fact, TestCategory("Kinesis"), TestCategory("Streaming")]
        public async Task Recoverable100EventStreamsWith1NonTransientErrorTest()
        {
            logger.Info("************************ Recoverable100EventStreamsWith1NonTransientErrorTest *********************************");
            await runner.Recoverable100EventStreamsWith1NonTransientError(GenerateEvents, ImplicitSubscription_NonTransientError_RecoverableStream_CollectorGrain.StreamNamespace, 4, 100);
        }

        private async Task GenerateEvents(string streamNamespace, int streamCount, int eventsInStream)
        {
            IStreamProvider streamProvider = GrainClient.GetStreamProvider(StreamProviderName);
            IAsyncStream<GeneratedEvent>[] producers =
                Enumerable.Range(0, streamCount)
                    .Select(i => streamProvider.GetStream<GeneratedEvent>(Guid.NewGuid(), streamNamespace))
                    .ToArray();

            for (int i = 0; i < eventsInStream - 1; i++)
            {
                // send event on each stream
                for (int j = 0; j < streamCount; j++)
                {
                    await producers[j].OnNextAsync(new GeneratedEvent { EventType = GeneratedEvent.GeneratedEventType.Fill });
                }
            }
            // send end events
            for (int j = 0; j < streamCount; j++)
            {
                await producers[j].OnNextAsync(new GeneratedEvent { EventType = GeneratedEvent.GeneratedEventType.Report });
            }
        }
    }
}
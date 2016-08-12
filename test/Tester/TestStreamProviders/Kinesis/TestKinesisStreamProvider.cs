
using Orleans.Kinesis.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.ServiceBus.Providers;

namespace Tester.TestStreamProviders.Kinesis
{
    public class TestKinesisStreamProvider : PersistentStreamProvider<TestKinesisStreamProvider.AdapterFactory>
    {
        public class AdapterFactory : KinesisAdapterFactory
        {
            public AdapterFactory()
            {
                StreamFailureHandlerFactory = qid => TestAzureTableStorageStreamFailureHandler.Create();
            }
        }
    }
}

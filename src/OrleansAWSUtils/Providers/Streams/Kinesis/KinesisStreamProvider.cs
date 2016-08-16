
using Orleans.Providers.Streams.Common;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Persistent stream provider that uses Kinesis for persistence
    ///  </summary>
    public class KinesisStreamProvider : PersistentStreamProvider<KinesisAdapterFactory>
    {
    }
}

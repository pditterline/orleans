
using System;
using Amazon.Kinesis.Model;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Interface for a stream message cache that stores Kinesis EventData
    /// </summary>
    public interface IKinesisQueueCache : IQueueFlowController, IDisposable
    {
        /// <summary>
        /// Add an Kinesis EventData to the cache.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="dequeueTimeUtc"></param>
        /// <returns></returns>
        StreamPosition Add(Record message, DateTime dequeueTimeUtc);
        /// <summary>
        /// Get a cursor into the cache to read events from a stream.
        /// </summary>
        /// <param name="streamIdentity"></param>
        /// <param name="sequenceToken"></param>
        /// <returns></returns>
        object GetCursor(IStreamIdentity streamIdentity, StreamSequenceToken sequenceToken);
        /// <summary>
        /// Try to get the next message in the cache for the provided cursor.
        /// </summary>
        /// <param name="cursorObj"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        bool TryGetNextMessage(object cursorObj, out IBatchContainer message);
    }
}


using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Amazon.Kinesis.Model;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using System.Numerics;
using Orleans.Providers;
using static System.String;
using Orleans.ServiceBus.Providers;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// This is a tightly packed cached structure containing a kinesis message.
    /// It should only contain value types.
    /// </summary>
    public struct CachedKinesisMessage
    {
        /// <summary>
        /// Guid of streamId this event is part of
        /// </summary>
        public Guid StreamGuid;
        /// <summary>
        /// Kinesis sequence number.
        /// </summary>
        public BigInteger SequenceNumber;
        /// <summary>
        /// Kinesis shard ID.
        /// </summary>
        public string ShardId;
        /// <summary>
        /// Time event was writen to Kinesis
        /// </summary>
        public DateTime EnqueueTimeUtc;
        /// <summary>
        /// Time event was read from Kinesis into this cache
        /// </summary>
        public DateTime DequeueTimeUtc;
        /// <summary>
        /// Segment containing the serialized event data
        /// </summary>
        public ArraySegment<byte> Segment;
    }

    /// <summary>
    /// Replication of Kinesis EventData class, reconstructed from cached data CachedKinesisMessage
    /// </summary>
    [Serializable]
    public class KinesisMessage
    {
        /// <summary>
        /// Duplicate of Kinesis's EventData class.
        /// </summary>
        /// <param name="cachedMessage"></param>
        public KinesisMessage(CachedKinesisMessage cachedMessage)
        {
            int readOffset = 0;
            StreamIdentity = new StreamIdentity(cachedMessage.StreamGuid, SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset));
            ShardId = cachedMessage.ShardId;
            SequenceNumber = BigInteger.Parse(SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset));
            EnqueueTimeUtc = cachedMessage.EnqueueTimeUtc;
            DequeueTimeUtc = cachedMessage.DequeueTimeUtc;                        
            Payload = SegmentBuilder.ReadNextBytes(cachedMessage.Segment, ref readOffset).ToArray();
        }

        /// <summary>
        /// Stream identifier.
        /// </summary>
        public IStreamIdentity StreamIdentity { get; }
        /// <summary>
        /// Kinesis shard ID.
        /// </summary>
        public string ShardId { get; }
        /// <summary>
        /// Kinesis sequence number.
        /// </summary>
        public BigInteger SequenceNumber { get; }
        /// <summary>
        /// Time event was written to Kinesis
        /// </summary>
        public DateTime EnqueueTimeUtc { get; }
        /// <summary>
        /// Time event was read from Kinesis and added to cache
        /// </summary>
        public DateTime DequeueTimeUtc { get; }
        /// <summary>
        /// Binary event data
        /// </summary>
        public byte[] Payload { get; }
    }

    /// <summary>
    /// Interface for Kinesis stream location.
    /// </summary>
    public interface IKinesisStreamPartitionLocation
    {
        /// <summary>
        /// Kinesis sequence number.
        /// </summary>
        BigInteger SequenceNumber { get; }
    }

    /// <summary>
    /// Stream sequence token for Kinesis streams.
    /// </summary>
    [Serializable]
    public class KinesisStreamSequenceToken : StreamSequenceToken, IKinesisStreamPartitionLocation
    {
        /// <summary>
        /// Kinesis sequence number.
        /// </summary>
        public BigInteger SequenceNumber { get; set; }
        /// <summary>
        /// Kinesis shard ID.
        /// </summary>
        public string ShardId { get; set; }

        /// <summary>
        /// Create a token from a Kinesis shard ID and sequence number.
        /// </summary>
        /// <param name="shardId"></param>
        /// <param name="sequenceNumber"></param>
        public KinesisStreamSequenceToken(string shardId, BigInteger sequenceNumber)
        {
            ShardId = shardId;
            SequenceNumber = sequenceNumber;
        }

        /// <summary>
        /// Check if another <see cref="StreamSequenceToken"/> equals this one.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public override bool Equals(StreamSequenceToken other)
        {
            var kinesisOther = other as KinesisStreamSequenceToken;

            if (ReferenceEquals(this, kinesisOther)) return true;            
            if (ReferenceEquals(kinesisOther, null)) return false;
            
            return SequenceNumber.Equals(kinesisOther.SequenceNumber) && String.Equals(ShardId, kinesisOther.ShardId);
        }

        /// <summary>
        /// Compare another <see cref="StreamSequenceToken"/> to this one.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public override int CompareTo(StreamSequenceToken other)
        {
            var kinesisOther = other as KinesisStreamSequenceToken;
            Debug.Assert(kinesisOther != null);

            var shardResult = Compare(ShardId, kinesisOther.ShardId, StringComparison.Ordinal);

            return shardResult == 0 ? SequenceNumber.CompareTo(kinesisOther.SequenceNumber) : shardResult;

        }
    }

    /// <summary>
    /// Default Kinesis data comparer.  Implements comparisions against CachedKinesisMessage
    /// </summary>
    internal class KinesisDataComparer : ICacheDataComparer<CachedKinesisMessage>
    {
        public static readonly ICacheDataComparer<CachedKinesisMessage> Instance = new KinesisDataComparer();

        public int Compare(CachedKinesisMessage cachedMessage, StreamSequenceToken token)
        {
            var realToken = (KinesisStreamSequenceToken)token;     
                   
            return (cachedMessage.SequenceNumber.CompareTo(realToken.SequenceNumber));
        }

        public bool Equals(CachedKinesisMessage cachedMessage, IStreamIdentity streamIdentity)
        {
            int result = cachedMessage.StreamGuid.CompareTo(streamIdentity.Guid);
            if (result != 0) return false;

            int readOffset = 0;
            string decodedStreamNamespace = SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset);
            return string.Compare(decodedStreamNamespace, streamIdentity.Namespace, StringComparison.Ordinal) == 0;
        }
    }

    /// <summary>
    /// Default Kinesis data adapter.  Users may subclass to override event data to stream mapping.
    /// </summary>
    public class KinesisDataAdapter : ICacheDataAdapter<Record, CachedKinesisMessage>
    {
        private readonly IObjectPool<FixedSizeBuffer> bufferPool;
        private readonly TimePurgePredicate timePurge;
        private FixedSizeBuffer currentBuffer;

        /// <summary>
        /// Assignable purge action.  This is called when a purge request is triggered.
        /// </summary>
        public Action<IDisposable> PurgeAction { private get; set; }

        /// <summary>
        /// Kinesis shard ID.
        /// </summary>
        protected readonly string _shardId;

        /// <summary>
        /// Cache data adapter that adapts Kinesis's Record to CachedKinesisMessage used in cache
        /// </summary>
        /// <param name="shardId"></param>
        /// <param name="bufferPool"></param>
        /// <param name="timePurge"></param>
        public KinesisDataAdapter(string shardId, IObjectPool<FixedSizeBuffer> bufferPool, TimePurgePredicate timePurge = null)
        {
            _shardId = shardId;
            if (bufferPool == null)
            {
                throw new ArgumentNullException("bufferPool");
            }
            this.bufferPool = bufferPool;
            this.timePurge = timePurge ?? TimePurgePredicate.Default;
        }

        /// <summary>
        /// Converts a TQueueMessage message from the queue to a TCachedMessage cachable structures.
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="queueMessage"></param>
        /// <param name="dequeueTimeUtc"></param>
        /// <returns></returns>
        public StreamPosition QueueMessageToCachedMessage(ref CachedKinesisMessage cachedMessage, Record queueMessage, DateTime dequeueTimeUtc)
        {
            //_shardId = cachedMessage.ShardId;
            StreamPosition streamPosition = GetStreamPosition(queueMessage);
            cachedMessage.StreamGuid = streamPosition.StreamIdentity.Guid;
            cachedMessage.ShardId = _shardId;
            cachedMessage.SequenceNumber = BigInteger.Parse(queueMessage.SequenceNumber);
            cachedMessage.EnqueueTimeUtc = queueMessage.ApproximateArrivalTimestamp;
            cachedMessage.DequeueTimeUtc = dequeueTimeUtc;
            cachedMessage.Segment = EncodeMessageIntoSegment(streamPosition, queueMessage);
            return streamPosition;
        }

        /// <summary>
        /// Converts a cached message to a batch container for delivery
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        public IBatchContainer GetBatchContainer(ref CachedKinesisMessage cachedMessage)
        {
            var evenHubMessage = new KinesisMessage(cachedMessage);
            return GetBatchContainer(evenHubMessage);
        }

        /// <summary>
        /// Convert a KinesisMessage to a batch container
        /// </summary>
        /// <param name="kinesisMessage"></param>
        /// <returns></returns>
        protected virtual IBatchContainer GetBatchContainer(KinesisMessage kinesisMessage)
        {
            return new KinesisBatchContainer(kinesisMessage);
        }

        /// <summary>
        /// Gets the stream sequence token from a cached message.
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        public virtual StreamSequenceToken GetSequenceToken(ref CachedKinesisMessage cachedMessage)
        {
            return new KinesisStreamSequenceToken(cachedMessage.ShardId, cachedMessage.SequenceNumber);
        }

        /// <summary>
        /// Gets the stream position from a queue message
        /// </summary>
        /// <param name="queueMessage"></param>
        /// <returns></returns>
        public virtual StreamPosition GetStreamPosition(Record queueMessage)
        {
            Guid streamGuid = queueMessage.GetStreamGuidProperty();
            string streamNamespace = queueMessage.GetStreamNamespaceProperty();
            IStreamIdentity streamIdentity = new StreamIdentity(streamGuid, streamNamespace);
            StreamSequenceToken token = new KinesisStreamSequenceToken(_shardId, BigInteger.Parse(queueMessage.SequenceNumber));

            return new StreamPosition(streamIdentity, token);
        }

        /// <summary>
        /// Given a purge request, indicates if a cached message should be purged from the cache
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="newestCachedMessage"></param>
        /// <param name="purgeRequest"></param>
        /// <param name="nowUtc"></param>
        /// <returns></returns>
        public bool ShouldPurge(ref CachedKinesisMessage cachedMessage, ref CachedKinesisMessage newestCachedMessage, IDisposable purgeRequest, DateTime nowUtc)
        {
            var purgedResource = (FixedSizeBuffer) purgeRequest;
            // if we're purging our current buffer, don't use it any more
            if (currentBuffer != null && currentBuffer.Id == purgedResource.Id)
            {
                currentBuffer = null;
            }

            TimeSpan timeInCache = nowUtc - cachedMessage.DequeueTimeUtc;
            // age of message relative to the most recent event in the cache.
            TimeSpan relativeAge = newestCachedMessage.EnqueueTimeUtc - cachedMessage.EnqueueTimeUtc;

            return ShouldPurgeFromResource(ref cachedMessage, purgedResource) || timePurge.ShouldPurgFromTime(timeInCache, relativeAge);
        }

        private static bool ShouldPurgeFromResource(ref CachedKinesisMessage cachedMessage, FixedSizeBuffer purgedResource)
        {
            // if message is from this resource, purge
            return cachedMessage.Segment.Array == purgedResource.Id;
        }

        private ArraySegment<byte> GetSegment(int size)
        {
            // get segment from current block
            ArraySegment<byte> segment;
            if (currentBuffer == null || !currentBuffer.TryGetSegment(size, out segment))
            {
                // no block or block full, get new block and try again
                currentBuffer = bufferPool.Allocate();
                currentBuffer.SetPurgeAction(PurgeAction);
                // if this fails with clean block, then requested size is too big
                if (!currentBuffer.TryGetSegment(size, out segment))
                {
                    string errmsg = Format(CultureInfo.InvariantCulture,
                        "Message size is to big. MessageSize: {0}", size);
                    throw new ArgumentOutOfRangeException("size", errmsg);
                }
            }
            return segment;
        }

        // Placed object message payload into a segment.
        private ArraySegment<byte> EncodeMessageIntoSegment(StreamPosition streamPosition, Record queueMessage)
        {
            byte[] payload = queueMessage.Data.ToArray();
            // get size of namespace, offset, properties, and payload
            int size = SegmentBuilder.CalculateAppendSize(streamPosition.StreamIdentity.Namespace) +
            SegmentBuilder.CalculateAppendSize(queueMessage.SequenceNumber) +
            SegmentBuilder.CalculateAppendSize(payload);

            // get segment
            ArraySegment<byte> segment = GetSegment(size);

            // encode namespace, offset, properties and payload into segment
            int writeOffset = 0;
            SegmentBuilder.Append(segment, ref writeOffset, streamPosition.StreamIdentity.Namespace);
            SegmentBuilder.Append(segment, ref writeOffset, queueMessage.SequenceNumber);
            SegmentBuilder.Append(segment, ref writeOffset, payload);

            return segment;
        }

    }
    
}

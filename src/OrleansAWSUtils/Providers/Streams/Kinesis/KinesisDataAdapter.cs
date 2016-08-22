
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

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// This is a tightly packed cached structure containing an event hub message.  
    /// It should only contain value types.
    /// </summary>
    public struct CachedKinesisMessage
    {
        public Guid StreamGuid;        
        public BigInteger SequenceNumber;
        public string ShardId;
        public DateTime EnqueueTimeUtc;
        public DateTime DequeueTimeUtc;
        public ArraySegment<byte> Segment;
    }

    /// <summary>
    /// Replication of Kinesis EventData class, reconstructed from cached data CachedKinesisMessage
    /// </summary>
    [Serializable]
    public class KinesisMessage
    {
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

        public IStreamIdentity StreamIdentity { get; }
        public string ShardId { get; }      
        public BigInteger SequenceNumber { get; }
        public DateTime EnqueueTimeUtc { get; }
        public DateTime DequeueTimeUtc { get; }
        public byte[] Payload { get; }
    }

    public interface IKinesisStreamPartitionLocation
    {        
        BigInteger SequenceNumber { get; }
    }

    [Serializable]
    internal class KinesisStreamSequenceToken : StreamSequenceToken, IKinesisStreamPartitionLocation
    {        
        public BigInteger SequenceNumber { get; set; }
        public string ShardId { get; set; }

        internal KinesisStreamSequenceToken(string shardId, BigInteger sequenceNumber)
        {
            ShardId = shardId;
            SequenceNumber = sequenceNumber;
        }

        public override bool Equals(StreamSequenceToken other)
        {
            var kinesisOther = other as KinesisStreamSequenceToken;

            if (ReferenceEquals(this, kinesisOther)) return true;            
            if (ReferenceEquals(kinesisOther, null)) return false;
            
            return SequenceNumber.Equals(kinesisOther.SequenceNumber) && String.Equals(ShardId, kinesisOther.ShardId);
        }

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
            return String.Compare(decodedStreamNamespace, streamIdentity.Namespace, StringComparison.Ordinal) == 0;
        }
    }

    /// <summary>
    /// Default event hub data adapter.  Users may subclass to override event data to stream mapping.
    /// </summary>
    public class KinesisDataAdapter : ICacheDataAdapter<Record, CachedKinesisMessage>
    {
        private readonly IObjectPool<FixedSizeBuffer> bufferPool;
        private FixedSizeBuffer currentBuffer;

        public Action<IDisposable> PurgeAction { private get; set; }

        private readonly string _shardId;

        public KinesisDataAdapter(string shardId, IObjectPool<FixedSizeBuffer> bufferPool)
        {
            _shardId = shardId;
            if (bufferPool == null)
            {
                throw new ArgumentNullException("bufferPool");
            }
            this.bufferPool = bufferPool;
        }

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

        public IBatchContainer GetBatchContainer(ref CachedKinesisMessage cachedMessage)
        {
            var evenHubMessage = new KinesisMessage(cachedMessage);
            return GetBatchContainer(evenHubMessage);
        }

        protected virtual IBatchContainer GetBatchContainer(KinesisMessage kinesisMessage)
        {
            return new KinesisBatchContainer(kinesisMessage);
        }

        public virtual StreamSequenceToken GetSequenceToken(ref CachedKinesisMessage cachedMessage)
        {
            return new KinesisStreamSequenceToken(cachedMessage.ShardId, cachedMessage.SequenceNumber);
        }

        public virtual StreamPosition GetStreamPosition(Record queueMessage)
        {
            Guid streamGuid = queueMessage.GetStreamGuidProperty();
            string streamNamespace = queueMessage.GetStreamNamespaceProperty();
            IStreamIdentity stremIdentity = new StreamIdentity(streamGuid, streamNamespace);
            StreamSequenceToken token = new KinesisStreamSequenceToken(_shardId, BigInteger.Parse(queueMessage.SequenceNumber));

            return new StreamPosition(stremIdentity, token);
        }

        public bool ShouldPurge(ref CachedKinesisMessage cachedMessage, IDisposable purgeRequest)
        {
            var purgedResource = (FixedSizeBuffer) purgeRequest;
            // if we're purging our current buffer, don't use it any more
            if (currentBuffer != null && currentBuffer.Id == purgedResource.Id)
            {
                currentBuffer = null;
            }
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

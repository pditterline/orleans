
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;
using Orleans.Kinesis.Providers;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers
{
    /// <summary>
    /// Batch container that is delivers payload and stream position information for a set of events in an EventHub EventData.
    /// </summary>
    [Serializable]
    public class KinesisBatchContainer : IBatchContainer
    {
        [JsonProperty]
        private readonly KinesisMessage record;

        [JsonProperty]
        private readonly KinesisStreamSequenceToken token;

        public Guid StreamGuid => record.StreamIdentity.Guid;
        public string StreamNamespace => record.StreamIdentity.Namespace;

        public StreamSequenceToken SequenceToken => token;

        // Payload is local cache of deserialized payloadBytes.  Should never be serialized as part of batch container.  During batch container serialization raw payloadBytes will always be used.
        [NonSerialized]
        private Body payload;
        private Body Payload => payload ?? (payload = SerializationManager.DeserializeFromByteArray<Body>(record.Payload));

        [Serializable]
        private class Body
        {
            public object Event { get; set; }
            public Dictionary<string, object> RequestContext { get; set; }
        }

        public KinesisBatchContainer(KinesisMessage record)
        {
            this.record = record;
            token = new KinesisStreamSequenceToken(record.ShardId, record.SequenceNumber);
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            yield return Tuple.Create<T, StreamSequenceToken>((T)Payload.Event, new KinesisStreamSequenceToken(token.ShardId, token.SequenceNumber));
        }

        public bool ImportRequestContext()
        {
            if (Payload.RequestContext != null)
            {
                RequestContext.Import(Payload.RequestContext);
                return true;
            }
            return false;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            return true;
        }

        internal static PutRecordsRequest ToPutRecordsRequest<T>(string streamName, IStreamIdentity streamIdentity, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            return new PutRecordsRequest
            {
                StreamName = streamName,
                Records = events.Select(e => ToRecordRequestEntry(e, streamIdentity, requestContext)).ToList()
            };
        }

        internal static PutRecordsRequestEntry ToRecordRequestEntry<T>(T requestedEvent, IStreamIdentity streamIdentity, Dictionary<string, object> requestContext)
        {
            var payload = new Body
            {
                Event = (object)requestedEvent,
                RequestContext = requestContext
            };
            var bytes = SerializationManager.SerializeToByteArray(payload);

            return new PutRecordsRequestEntry
            {
                Data = new MemoryStream(bytes),
                PartitionKey =
                    string.IsNullOrWhiteSpace(streamIdentity.Namespace)
                        ? streamIdentity.Guid.ToString()
                        : $"{streamIdentity.Namespace}.{streamIdentity.Guid}"
            };
        }

    }
}

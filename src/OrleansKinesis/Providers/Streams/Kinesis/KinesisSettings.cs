
using System;
using System.Collections.Generic;
using System.Globalization;
using Orleans.Providers;

namespace Orleans.Kinesis.Providers
{
    [Serializable]
    public class KinesisSettings : IKinesisSettings
    {
        private const string ConnectionStringName = "KinesisConnectionString";
        private const string ConsumerGroupName = "KinesisConsumerGroup";
        private const string PathName = "KinesisStreamName";        
        private const string StartFromNowName = "StartFromNow";
        private const bool StartFromNowDefault = true;

        public KinesisSettings(){}

        public KinesisSettings(string connectionString, string consumerGroup, string streamName, bool startFromNow = StartFromNowDefault, int? prefetchCount = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException("connectionString");
            }
            if (string.IsNullOrWhiteSpace(consumerGroup))
            {
                throw new ArgumentNullException("consumerGroup");
            }
            if (string.IsNullOrWhiteSpace(streamName))
            {
                throw new ArgumentNullException("streamName");
            }
            ConnectionString = connectionString;
            ConsumerGroup = consumerGroup;
            StreamName = streamName;
            StartFromNow = startFromNow;
        }

        public string ConnectionString { get; private set; }
        public string ConsumerGroup { get; private set; }
        public string StreamName { get; private set; }
        public bool StartFromNow { get; private set; }

        /// <summary>
        /// Utility function to convert config to property bag for use in stream provider configuration
        /// </summary>
        /// <returns></returns>
        public void WriteProperties(Dictionary<string, string> properties)
        {
            properties.Add(ConnectionStringName, ConnectionString);
            properties.Add(ConsumerGroupName, ConsumerGroup);
            properties.Add(PathName, StreamName);
            properties.Add(StartFromNowName, StartFromNow.ToString());
        }

        /// <summary>
        /// Utility function to populate config from provider config
        /// </summary>
        /// <param name="providerConfiguration"></param>
        public virtual void PopulateFromProviderConfig(IProviderConfiguration providerConfiguration)
        {
            ConnectionString = providerConfiguration.GetProperty(ConnectionStringName, null);
            if (string.IsNullOrWhiteSpace(ConnectionString))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", ConnectionStringName + " not set.");
            }
            ConsumerGroup = providerConfiguration.GetProperty(ConsumerGroupName, null);
            if (string.IsNullOrWhiteSpace(ConsumerGroup))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", ConsumerGroupName + " not set.");
            }
            StreamName = providerConfiguration.GetProperty(PathName, null);
            if (string.IsNullOrWhiteSpace(StreamName))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", PathName + " not set.");
            }
            StartFromNow = providerConfiguration.GetBoolProperty(StartFromNowName, StartFromNowDefault);
        }
    }
}

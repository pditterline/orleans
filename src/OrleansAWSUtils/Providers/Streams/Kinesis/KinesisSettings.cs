
using System;
using System.Collections.Generic;
using Amazon.Kinesis;
using Orleans.Providers;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Kinesis settings for a specific stream.
    /// </summary>
    [Serializable]
    public class KinesisSettings : IKinesisSettings
    {
        private const string ConnectionStringName = "KinesisConnectionString";        
        private const string PathName = "KinesisStreamName";        
        private const string StartFromNowName = "StartFromNow";
        private const bool StartFromNowDefault = true;

        /// <summary>
        /// Default constructor
        /// </summary>
        public KinesisSettings(){}

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="kinesisConfig"></param>
        /// <param name="streamName"></param>
        /// <param name="startFromNow"></param>
        public KinesisSettings(string kinesisConfig, string streamName, bool startFromNow = StartFromNowDefault)
        {
            if (string.IsNullOrWhiteSpace(kinesisConfig))
            {
                throw new ArgumentNullException("kinesisConfig");
            }            
            if (string.IsNullOrWhiteSpace(streamName))
            {
                throw new ArgumentNullException("streamName");
            }
            KinesisConfig = new AmazonKinesisConfig {ServiceURL = kinesisConfig};
            StreamName = streamName;
            StartFromNow = startFromNow;
        }

        /// <summary>
        /// Kinesis connection settings.
        /// </summary>
        public AmazonKinesisConfig KinesisConfig { get; private set; }
        /// <summary>
        /// Consumer group
        /// TODO: does this have actually have a Kinesis counterpart?
        /// </summary>
        public string ConsumerGroup { get; private set; }
        /// <summary>
        /// Stream name.
        /// </summary>
        public string StreamName { get; private set; }
        /// <summary>
        /// In cases where no checkpoint is found, this indicates if service should read from the most recent data, or from the begining of a partition.
        /// </summary>
        public bool StartFromNow { get; private set; }

        /// <summary>
        /// Utility function to convert config to property bag for use in stream provider configuration
        /// </summary>
        /// <returns></returns>
        public void WriteProperties(Dictionary<string, string> properties)
        {
            properties.Add(ConnectionStringName, KinesisConfig.ServiceURL);
            properties.Add(PathName, StreamName);
            properties.Add(StartFromNowName, StartFromNow.ToString());
        }

        /// <summary>
        /// Utility function to populate config from provider config
        /// </summary>
        /// <param name="providerConfiguration"></param>
        public virtual void PopulateFromProviderConfig(IProviderConfiguration providerConfiguration)
        {
            var savedConfigString = providerConfiguration.GetProperty(ConnectionStringName, null);
            
            if (string.IsNullOrWhiteSpace(savedConfigString))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", ConnectionStringName + " not set.");
            }
            KinesisConfig = new AmazonKinesisConfig {ServiceURL = savedConfigString};

            StreamName = providerConfiguration.GetProperty(PathName, null);
            if (string.IsNullOrWhiteSpace(StreamName))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", PathName + " not set.");
            }
            StartFromNow = providerConfiguration.GetBoolProperty(StartFromNowName, StartFromNowDefault);
        }
    }    
}

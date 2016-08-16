
using System;
using System.Collections.Generic;
using System.Globalization;
using Amazon.Kinesis;
using Orleans.Providers;
using Newtonsoft.Json;

namespace Orleans.Kinesis.Providers
{
    [Serializable]
    public class KinesisSettings : IKinesisSettings
    {
        private const string ConnectionStringName = "KinesisConnectionString";        
        private const string PathName = "KinesisStreamName";        
        private const string StartFromNowName = "StartFromNow";
        private const bool StartFromNowDefault = true;

        public KinesisSettings(){}

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

        public AmazonKinesisConfig KinesisConfig { get; private set; }
        public string ConsumerGroup { get; private set; }
        public string StreamName { get; private set; }
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

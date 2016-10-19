
using System;
using System.Collections.Generic;
using System.Globalization;
using Orleans.Providers;

namespace Orleans.Kinesis.Providers
{
    /// <summary>
    /// Configuration settings for the Kinesis stream provider.
    /// </summary>
    public class KinesisStreamProviderConfig
    {
        /// <summary>
        /// KinesisSettingsType setting name.
        /// </summary>
        public const string KinesisConfigTypeName = "KinesisSettingsType";
        /// <summary>
        /// EventHub configuration type.  Type must conform to IEventHubSettings interface.
        /// </summary>
        public Type KinesisSettingsType { get; set; }

        /// <summary>
        /// CheckpointerSettingsType setting name.
        /// </summary>
        public const string CheckpointerSettingsTypeName = "CheckpointerSettingsType";
        /// <summary>
        /// Checkpoint settings type.  Type must conform to ICheckpointerSettings interface.
        /// </summary>
        public Type CheckpointerSettingsType { get; set; }

        /// <summary>
        /// Stream provider name.  This setting is required.
        /// </summary>
        public string StreamProviderName { get; private set; }

        /// <summary>
        /// CacheSizeMb setting name.
        /// </summary>
        public const string CacheSizeMbName = "CacheSizeMb";
        private const int DefaultCacheSizeMb = 128; // default to 128mb cache.
        /// <summary>
        /// Cache size in megabytes.
        /// </summary>
        public int CacheSizeMb { get; set; }

        /// <summary>
        /// DataMinTimeInCache setting name.
        /// </summary>
        public const string DataMinTimeInCacheName = "DataMinTimeInCache";
        private static readonly TimeSpan DefaultDataMinTimeInCache = TimeSpan.FromMinutes(5);
        private TimeSpan? dataMinTimeInCache;
        /// <summary>
        /// Minimum time message will stay in cache before it is available for time based purge.
        /// </summary>
        public TimeSpan DataMinTimeInCache
        {
            get { return dataMinTimeInCache ?? DefaultDataMinTimeInCache; }
            set { dataMinTimeInCache = value; }
        }

        /// <summary>
        /// DataMaxAgeInCache setting name.
        /// </summary>
        public const string DataMaxAgeInCacheName = "DataMaxAgeInCache";
        private static readonly TimeSpan DefaultDataMaxAgeInCache = TimeSpan.FromMinutes(30);
        private TimeSpan? dataMaxAgeInCache;
        /// <summary>
        /// Difference in time between the newest and oldest messages in the cache.  Any messages older than this will be purged from the cache.
        /// </summary>
        public TimeSpan DataMaxAgeInCache
        {
            get { return dataMaxAgeInCache ?? DefaultDataMaxAgeInCache; }
            set { dataMaxAgeInCache = value; }
        }

        /// <summary>
        /// Constructor.  Requires provider name.
        /// </summary>
        /// <param name="streamProviderName"></param>
        /// <param name="cacheSizeMb"></param>
        public KinesisStreamProviderConfig(string streamProviderName, int cacheSizeMb = DefaultCacheSizeMb)
        {
            StreamProviderName = streamProviderName;
            CacheSizeMb = cacheSizeMb;
        }

        /// <summary>
        /// Writes settings into a property bag.
        /// </summary>
        /// <param name="properties"></param>
        public void WriteProperties(Dictionary<string, string> properties)
        {
            if (KinesisSettingsType != null)
                properties.Add(KinesisConfigTypeName, KinesisSettingsType.AssemblyQualifiedName);
            if (CheckpointerSettingsType != null)
                properties.Add(CheckpointerSettingsTypeName, CheckpointerSettingsType.AssemblyQualifiedName);
            properties.Add(CacheSizeMbName, CacheSizeMb.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Read settings from provider configuration.
        /// </summary>
        /// <param name="providerConfiguration"></param>
        public void PopulateFromProviderConfig(IProviderConfiguration providerConfiguration)
        {
            KinesisSettingsType = providerConfiguration.GetTypeProperty(KinesisConfigTypeName, null);
            CheckpointerSettingsType = providerConfiguration.GetTypeProperty(CheckpointerSettingsTypeName, null);
            if (string.IsNullOrWhiteSpace(StreamProviderName))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", "StreamProviderName not set.");
            }
            CacheSizeMb = providerConfiguration.GetIntProperty(CacheSizeMbName, DefaultCacheSizeMb);
        }

        /// <summary>
        /// Aquire configured IKinesisSettings class
        /// </summary>
        /// <param name="providerConfig"></param>
        /// <param name="serviceProvider"></param>
        /// <returns></returns>
        public IKinesisSettings GetKinesisSettings(IProviderConfiguration providerConfig, IServiceProvider serviceProvider)
        {
            // if no kinesis settings type is provided, use KinesisSettings and get populate settings from providerConfig
            if (KinesisSettingsType == null)
            {
                KinesisSettingsType = typeof(KinesisSettings);
            }

            var hubSettings = (IKinesisSettings)(serviceProvider?.GetService(KinesisSettingsType) ?? Activator.CreateInstance(KinesisSettingsType));
            if (hubSettings == null)
            {
                throw new ArgumentOutOfRangeException(nameof(providerConfig), "KinesisSettingsType not valid.");
            }

            // if settings is an KinesisSettings class, populate settings from providerConfig
            var settings = hubSettings as KinesisSettings;
            settings?.PopulateFromProviderConfig(providerConfig);

            return hubSettings;
        }

        /// <summary>
        /// Aquire configured ICheckpointerSettings class
        /// </summary>
        /// <param name="providerConfig"></param>
        /// <param name="serviceProvider"></param>
        /// <returns></returns>
        public ICheckpointerSettings GetCheckpointerSettings(IProviderConfiguration providerConfig, IServiceProvider serviceProvider)
        {
            // if no checkpointer settings type is provided, use KinesisCheckpointerSettings and get populate settings from providerConfig
            if (CheckpointerSettingsType == null)
            {
                CheckpointerSettingsType = typeof(KinesisCheckpointerSettings);
            }

            var checkpointerSettings = (ICheckpointerSettings)(serviceProvider?.GetService(CheckpointerSettingsType) ?? Activator.CreateInstance(CheckpointerSettingsType));
            if (checkpointerSettings == null)
            {
                throw new ArgumentOutOfRangeException(nameof(providerConfig), "CheckpointerSettingsType not valid.");
            }

            // if settings is an KinesisCheckpointerSettings class, populate settings from providerConfig
            var settings = checkpointerSettings as KinesisCheckpointerSettings;
            settings?.PopulateFromProviderConfig(providerConfig);

            return checkpointerSettings;
        }
    }
}

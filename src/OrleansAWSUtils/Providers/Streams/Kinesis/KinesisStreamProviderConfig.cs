
using System;
using System.Collections.Generic;
using System.Globalization;
using Orleans.Providers;

namespace Orleans.Kinesis.Providers
{
    public class KinesisStreamProviderConfig
    {
        public const string KinesisConfigTypeName = "KinesisSettingsType";
        public Type KinesisSettingsType { get; set; }

        public const string CheckpointerSettingsTypeName = "CheckpointerSettingsType";
        public Type CheckpointerSettingsType { get; set; }

        public string StreamProviderName { get; private set; }

        public const string CacheSizeMbName = "CacheSizeMb";
        private const int DefaultCacheSizeMb = 128; // default to 128mb cache.
        public int CacheSizeMb { get; private set; }

        public KinesisStreamProviderConfig(string streamProviderName, int cacheSizeMb = DefaultCacheSizeMb)
        {
            StreamProviderName = streamProviderName;
            CacheSizeMb = cacheSizeMb;
        }

        public void WriteProperties(Dictionary<string, string> properties)
        {
            if (KinesisSettingsType != null)
                properties.Add(KinesisConfigTypeName, KinesisSettingsType.AssemblyQualifiedName);
            if (CheckpointerSettingsType != null)
                properties.Add(CheckpointerSettingsTypeName, CheckpointerSettingsType.AssemblyQualifiedName);
            properties.Add(CacheSizeMbName, CacheSizeMb.ToString(CultureInfo.InvariantCulture));
        }

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

        public IKinesisSettings GetKinesisSettings(IProviderConfiguration providerConfig, IServiceProvider serviceProvider)
        {
            // if no event hub settings type is provided, use KinesisSettings and get populate settings from providerConfig
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
            if (settings != null)
            {
                settings.PopulateFromProviderConfig(providerConfig);
            }

            return hubSettings;
        }

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
            if (settings != null)
            {
                settings.PopulateFromProviderConfig(providerConfig);
            }

            return checkpointerSettings;
        }
    }
}

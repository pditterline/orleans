using System;
using System.Collections.Generic;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Runtime.Configuration
{
    /// <summary>
    /// Extension methods for AWS providers.
    /// </summary>
    public static class AWSConfigurationExtensions
    {
        /// <summary>
        /// Adds a storage provider of type <see cref="DynamoDBStorageProvider"/>.
        /// </summary>
        /// <param name="config">The cluster configuration object to add provider to.</param>
        /// <param name="providerName">The provider name.</param>
        /// <param name="connectionString">The azure storage connection string. If none is provided, it will use the same as in the Globals configuration.</param>
        /// <param name="tableName">The table name where to store the state.</param>
        /// <param name="deleteOnClear">Whether the provider deletes the state when <see cref="IStorageProvider.ClearStateAsync"/> is called.</param>
        /// <param name="useJsonFormat">Whether is stores the content as JSON or as binary in Azure Table.</param>
        /// <param name="useFullAssemblyNames">Whether to use full assembly names in the serialized JSON. This value is ignored if <paramref name="useJsonFormat"/> is false.</param>
        /// <param name="indentJson">Whether to indent (pretty print) the JSON. This value is ignored if <paramref name="useJsonFormat"/> is false.</param>
        public static void AddDynamoDbStorageProvider(
            this ClusterConfiguration config,
            string providerName = "AzureTableStore",
            string connectionString = null,
            string tableName = DynamoDBStorageProvider.TABLE_NAME_DEFAULT_VALUE,
            bool deleteOnClear = false,
            bool useJsonFormat = false,
            bool useFullAssemblyNames = false,
            bool indentJson = false)
        {
            if (string.IsNullOrWhiteSpace(providerName)) throw new ArgumentNullException(nameof(providerName));
            connectionString = GetConnectionString(connectionString, config);

            var properties = new Dictionary<string, string>
            {
                { DynamoDBStorageProvider.DATA_CONNECTION_STRING_PROPERTY_NAME, connectionString },
                { DynamoDBStorageProvider.TABLE_NAME_PROPERTY_NAME, tableName },
                { DynamoDBStorageProvider.DELETE_ON_CLEAR_PROPERTY_NAME, deleteOnClear.ToString() },
                { DynamoDBStorageProvider.USE_JSON_FORMAT_PROPERTY_NAME, useJsonFormat.ToString() },
            };

            if (useJsonFormat)
            {
                properties.Add(OrleansJsonSerializer.UseFullAssemblyNamesProperty, useFullAssemblyNames.ToString());
                properties.Add(OrleansJsonSerializer.IndentJsonProperty, indentJson.ToString());
            }

            config.Globals.RegisterStorageProvider<DynamoDBStorageProvider>(providerName, properties);
        }

        private static string GetConnectionString(string connectionString, ClusterConfiguration config)
        {
            if (!string.IsNullOrWhiteSpace(connectionString)) return connectionString;
            if (!string.IsNullOrWhiteSpace(config.Globals.DataConnectionString)) return config.Globals.DataConnectionString;

            throw new ArgumentNullException(nameof(connectionString),
                "Parameter value and fallback value are both null or empty.");
        }
    }
}

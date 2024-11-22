namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderProcessingJobs
{
    using global::Azure.Storage.Blobs;
    using global::Azure.Storage.Sas;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureResourceManagement.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureResourceManagement.Contracts.Models.GenericResource;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureResourceManagement.Enums;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureResourceManagement.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Jobs.Base;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts.EventGrid.Arn;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceMonitoring.Ifx;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Utils;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// ShredderProcessingJob
    /// </summary>
    internal class ShredderProcessingJob : PassthroughJob<ShredderProcessingJobContext>
    {
        #region Tracing

        private static readonly ActivityMonitorFactory ShredderProcessingDataFileJobExecuteInternalPassthroughAsync =
            new ActivityMonitorFactory("ShredderProcessingJob.ExecuteInternalPassthroughAsync");

        private static readonly ActivityMonitorFactory ShredderProcessingJobUpdateConfig =
            new ActivityMonitorFactory("ShredderProcessingJob.UpdateConfig");

        #endregion

        #region Private Members

        private static Dictionary<string, bool> ConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled;

        private static Dictionary<string, List<string>> ConfigDictionarySnapshotResourceTypeToAllowedTenantIds;

        private static Dictionary<string, int> ConfigDictionarySnapshotResourceTypeToProcessingTime;

        private static Dictionary<string, string> ConfigDictionarySnapshotResourceTypeToARNSchemaVersion;

        private static bool GenerateBlobForPartitionFileMetadata;

        #endregion

        static ShredderProcessingJob()
        {
            ServiceConfiguration.Current.Requirement
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToShredderBigProcessingJobEnabled)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToAllowedTenantIds)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToProcessingTime)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToARNSchemaVersion)
                .Add<bool>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.GenerateBlobForPartitionFileMetadata)
                .RegisterCallbackAndInitialize(UpdateConfig);
        }

        public ShredderProcessingJob()
        {
        }

        protected override async Task ExecuteInternalPassthroughAsync(ShredderProcessingJobContext context, CancellationToken cancellationToken, IActivity parentActivity)
        {
            GuardHelper.ArgumentNotNull(context);
            GuardHelper.ArgumentNotNull(context.QueueMessage);
            GuardHelper.ArgumentNotNull(context.Message);
            GuardHelper.ArgumentNotNull(context.Message.NotificationMessage);
            GuardHelper.ArgumentNotNull(context.Message.NotificationMessage.ResourceType);

            var methodMonitor = ShredderProcessingDataFileJobExecuteInternalPassthroughAsync.ToMonitor(parentActivity);
            methodMonitor.Activity["MessageId"] = context.QueueMessage.MessageId;
            methodMonitor.Activity["PopReceipt"] = context.QueueMessage.PopReceipt;
            methodMonitor.Activity["DequeueCount"] = context.QueueMessage.DequeueCount;
            methodMonitor.Activity["InsertionTime"] = context.QueueMessage.InsertionTime;
            methodMonitor.Activity["NotificationSnapshotTime"] = context.Message.NotificationSnapshotTime;
            methodMonitor.Activity["PartnerBlobUri"] = BlobLoggingUtils.HideSigFromBlobUri(context.Message.PartnerBlobUri);
            methodMonitor.Activity["Version"] = context.Message.Version;
            methodMonitor.Activity["ResourceType"] = context.Message.NotificationMessage.ResourceType;
            methodMonitor.Activity["KustoPartitions"] = context.Message.KustoPartitions;
            methodMonitor.Activity["CorrelationId"] = context.Message.CorrelationId;
            methodMonitor.Activity["SegmentIndex"] = context.Message.SegmentIndex;

            methodMonitor.OnStart();

            try
            {
                if (ConfigDictionarySnapshotResourceTypeToARNSchemaVersion[context.Message.NotificationMessage.ResourceType] == "5.0")
                {
                    methodMonitor.Activity["SchemaUsed"] = "V5";
                    await this.ProcessMessageAsync<NotificationResourceDataV5>(context, cancellationToken, methodMonitor.Activity).IgnoreContext();
                }
                else
                {
                    methodMonitor.Activity["SchemaUsed"] = "V3";
                    await this.ProcessMessageAsync<NotificationResourceData>(context, cancellationToken, methodMonitor.Activity).IgnoreContext();
                }

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                methodMonitor.OnError(ex);
                throw;
            }
        }

        protected async Task ProcessMessageAsync<T>(ShredderProcessingJobContext context, CancellationToken cancellationToken, IActivity parentActivity) where T : NotificationResourceData
        {
            var snapshotContainer = context.BlobClient.GetContainerReference(context.Message.SnapshotContainerName);

            if (!ConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled[context.Message.NotificationMessage.ResourceType])
            {
                parentActivity["JobDisabled"] = bool.TrueString;
                return;
            }

            if (!await snapshotContainer.ExistsAsync(cancellationToken).IgnoreContext())
            {
                parentActivity["SnapshotContainerExists"] = bool.FalseString;
                return;
            }

            var completedFile = snapshotContainer.GetBlockBlobReference($"{context.Message.SnapshotBlobFolder}/{ConfigurationConstants.CompletedFileName}");

            if (await completedFile.ExistsAsync(cancellationToken).IgnoreContext())
            {
                parentActivity["EntireSnapshotAlreadyDone"] = bool.TrueString;
                return;
            }

            var completedFileForJob = snapshotContainer.GetBlockBlobReference($"{context.Message.SnapshotBlobFolder}/{ConfigurationConstants.CompletedFileName}_{context.Message.SegmentIndex}");

            if (await completedFileForJob.ExistsAsync(cancellationToken).IgnoreContext())
            {
                parentActivity["ProcessingDoneByJob"] = bool.TrueString;
                return;
            }

            var snapshotBlob = snapshotContainer.GetBlobReference(context.Message.SnapshotBlobPath);
            var stream = await MultiLineContentBlobUtilities.GetSegmentStreamAsync(
                snapshotBlob, context.Message.TotalSegments,
                context.Message.SegmentIndex, cancellationToken).IgnoreContext();
            var kustoPartitionScopeTypeToNotificationResourceData = new SortedDictionary<(int Partition, ScopeType ScopeType), List<T>>();
            var invalidResourceIds = new List<string>();
            var totalNumberOfResourcesInSnapshotFile = 0;

            var dimensions = new[] { "DataType", context.Message.NotificationMessage.ResourceType };

            // In case of a small snapshot where a segment does not have any data skip over doing remote calls.
            if (stream != null)
            {
                var jsonReader = new JsonTextReader(new StreamReader(stream))
                {
                    SupportMultipleContent = true
                };

                // Read all data in segment and partition by tenant prefix.
                var jsonSerializer = JsonSerializer.Create();
                while (jsonReader.Read())
                {
                    var notificationResourceData = jsonSerializer.Deserialize<T>(jsonReader);
                    totalNumberOfResourcesInSnapshotFile++;

                    if (ConfigDictionarySnapshotResourceTypeToAllowedTenantIds.ContainsKey(context.Message.NotificationMessage.ResourceType)
                        && !ConfigDictionarySnapshotResourceTypeToAllowedTenantIds[context.Message.NotificationMessage.ResourceType]
                            .Contains(notificationResourceData.HomeTenantId, StringComparer.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    ScopeType? storeScopeType;
                    string scopeId;

                    if (notificationResourceData.IsBuiltIn())
                    {
                        storeScopeType = ScopeType.BuiltIn;
                        scopeId = ScopeHelper.BuiltInScopeId;
                    }
                    else
                    {
                        scopeId = ScopeHelper.BuildScopeId(
                            notificationResourceData.ResourceHomeTenantId ?? notificationResourceData.HomeTenantId,
                            notificationResourceData.ResourceId,
                            out storeScopeType);
                    }

                    if (scopeId == null || storeScopeType == null)
                    {
                        invalidResourceIds.Add(notificationResourceData.ResourceId);
                        continue;
                    }

                    // Sometimes in snapshot we get resourceIds which are guids but they are without hyphen '-'.
                    // So, explicitly converting any resourceId that ends with a guid (without hyphen) to a guid (with hyphen)
                    if(TryConvertResourceIdInGuidFormat(notificationResourceData.ResourceId, out var resourceId))
                    {
                        notificationResourceData.ResourceId = resourceId;
                    }

                    if (TryConvertResourceIdInGuidFormat(notificationResourceData.ArmResource.Id, out var armResourceId))
                    {
                        notificationResourceData.UpdateArmResource(GetArmResourceInGuidFormat(notificationResourceData.ArmResource, armResourceId));
                    }

                    var partitionKey = HashUtils.GetRangePartitionForScopeId(scopeId, context.Message.KustoPartitions);

                    if (!kustoPartitionScopeTypeToNotificationResourceData.TryGetValue((partitionKey, storeScopeType.Value), out var list))
                    {
                        list = new List<T>();
                        kustoPartitionScopeTypeToNotificationResourceData[(partitionKey, storeScopeType.Value)] = list;
                    }
                    
                    list.Add(notificationResourceData);
                }
            }

            parentActivity.LogCollectionAndCount("InvalidResourceIds", invalidResourceIds, 100);

            IfxMetricLogger.LogMetricValue(
                IfxTracingConstants.ShredderSnapshotTotalNumberOfResourcesInSnapshotMetric,
                totalNumberOfResourcesInSnapshotFile,
                parentActivity: parentActivity,
                metricDimensionNamesValues: dimensions);

            var shreddedNotificationMessages = new List<ArgNotificationMessage>();
            var partitionDataFileMetadataBlobNames = new List<string>();

            foreach (var resourceData in kustoPartitionScopeTypeToNotificationResourceData)
            {
                var partitionDataFileBlob = snapshotContainer.GetBlockBlobReference($"{context.Message.SnapshotBlobFolder}/{resourceData.Key.Partition}/{resourceData.Key.ScopeType}/{ConfigurationConstants.PartitionDataFileName}_{context.Message.SegmentIndex}{ConfigurationConstants.JsonFileType}");
                partitionDataFileMetadataBlobNames.Add($"{context.Message.SnapshotBlobFolder}/{resourceData.Key.Partition}/{resourceData.Key.ScopeType}/{ConfigurationConstants.PartitionDataFileName}_{context.Message.SegmentIndex}_PS{ConfigurationConstants.JsonFileType}");
                await partitionDataFileBlob.SerializeAndUploadAsync(
                    resourceData.Value,
                    cancellationToken).IgnoreContext();

                await partitionDataFileBlob.FetchAttributesAsync(cancellationToken).IgnoreContext();

                var message = GetShreddedArgNotification(
                        await partitionDataFileBlob.GetBlobUriWithSasCredentialsAsync(
                            TimeSpan.FromDays(7),
                            BlobSasPermissions.Read).IgnoreContext(),
                        partitionDataFileBlob.Properties.Length.ToString(),
                        context.Message.NotificationMessage);

                shreddedNotificationMessages.Add(message);

                IfxMetricLogger.LogMetricValue(
                    IfxTracingConstants.ShredderSnapshotNumberOfResourcesIngestedMetric,
                    resourceData.Value.Count,
                    parentActivity: parentActivity,
                    metricDimensionNamesValues: new[] 
                        { "DataType", context.Message.NotificationMessage.ResourceType,
                          "PartitionNumber", resourceData.Key.Partition.ToString(),
                          "ScopeType", resourceData.Key.ScopeType.ToString()
                        });
            }

            (var delta, var numberOfMessagesInGroup) = ShredderUtils.GetParametersToQueueMessagesOverPeriodOfTime(ConfigDictionarySnapshotResourceTypeToProcessingTime[context.Message.NotificationMessage.ResourceType], shreddedNotificationMessages.Count + 1);

            parentActivity["MaximumTimeToProcess"] = ConfigDictionarySnapshotResourceTypeToProcessingTime[context.Message.NotificationMessage.ResourceType];
            parentActivity["TotalMessages"] = shreddedNotificationMessages.Count + 1;
            parentActivity["Delta"] = delta;
            parentActivity["NumberOfMessagesInGroup"] = numberOfMessagesInGroup;

            for (var index = 0; index < shreddedNotificationMessages.Count; index++)
            {
                var shreddedNotification = shreddedNotificationMessages[index];
                var visibilityDelay = (index / numberOfMessagesInGroup) * delta;

                if (GenerateBlobForPartitionFileMetadata)
                {
                    var partitionDataFileMetadataBlobName = partitionDataFileMetadataBlobNames[index];
                    var partitionDataFileMetadataBlob = snapshotContainer.GetBlockBlobReference(partitionDataFileMetadataBlobName);

                    var partitionDataFileMetadata = $"VisibilityDelay_{index}: {visibilityDelay}," +
                        $"SnapshotNotificationRetryQueueAccountName_{index}: {context.SnapshotNotificationRetryQueue.AccountName}," +
                        $"SnapshotNotificationRetryQueueName_{index}: {context.SnapshotNotificationRetryQueue.Name}";

                    await partitionDataFileMetadataBlob.SerializeAndUploadAsync(partitionDataFileMetadata, cancellationToken).IgnoreContext();
                }

                await context.SnapshotNotificationRetryQueue.AddMessageAsync(
                    new QueueMessage<ArgRetryNotificationMessage>(
                        new ArgRetryNotificationMessage(
                            shreddedNotification,
                            RetryNotificationPriority.Snapshot,
                            null,
                            TimeSpan.Zero)),
                           cancellationToken,
                    TimeSpan.FromDays(2),
                    TimeSpan.FromSeconds(visibilityDelay))
                   .IgnoreContext();
            }

            await completedFileForJob.UploadFromStreamAsync(new MemoryStream(), cancellationToken).IgnoreContext();

            parentActivity.Properties["AllPartitionedMessagesQueued"] = bool.TrueString;
        }

        private bool TryConvertResourceIdInGuidFormat(string resourceId, out string resultResourceId)
        {
            if (Guid.TryParse(resourceId.Substring(resourceId.LastIndexOf('/') + 1), out var guidFormatted))
            {
                resultResourceId = string.Concat(
                    resourceId.AsSpan().Slice(0, resourceId.LastIndexOf('/') + 1),
                    guidFormatted.ToString());

                return true;
            }

            resultResourceId = resourceId;
            return false;
        }

        private GenericResource GetArmResourceInGuidFormat(GenericResource armResource, string armResourceId)
        {
            return new GenericResource(
                armResourceId,
                armResource.Name,
                armResource.Type,
                armResource.Location, 
                armResource.Tags,
                armResource.Plan,
                armResource.Properties,
                armResource.Kind,
                armResource.ManagedBy,
                armResource.Sku,
                armResource.Identity,
                armResource.Zones,
                armResource.SystemData,
                armResource.ExtendedLocation,
                armResource.DisplayName,
                armResource.ApiVersion
                );
        }

        private ArgNotificationMessage GetShreddedArgNotification(
            string dataUri,
            string blobSize,
            ArgNotificationMessage argNotificationMessage)
        {
            return new ArgNotificationMessage(scopeId: argNotificationMessage.ResourceId,
                action: argNotificationMessage.Action,
                armCorrelationId: argNotificationMessage.ArmCorrelationId,
                providerNamespace: argNotificationMessage.ProviderNamespace,
                resourceType: argNotificationMessage.ResourceType,
                notificationTimestamp: argNotificationMessage.Timestamp,
                notificationReceptionTimestamp: argNotificationMessage.NotificationReceptionTimestamp,
                armEventTimestamp: argNotificationMessage.ArmEventTimestamp,
                notificationSource: argNotificationMessage.NotificationSource,
                routingType: argNotificationMessage.RoutingType,
                dataUri: dataUri,
                blobSize: blobSize,
                notificationSubject: argNotificationMessage.NotificationSubject,
                publisherInfo: argNotificationMessage.PublisherInfo,
                partitionTag: argNotificationMessage.PartitionTag,
                resourceLocation: argNotificationMessage.ResourceLocation,
                frontdoorLocation: argNotificationMessage.FrontdoorLocation,
                homeTenantId: argNotificationMessage.HomeTenantId,
                resourceHomeTenantId: argNotificationMessage.ClientTenantId,
                eventDataVersion: argNotificationMessage.EventDataVersion,
                isReplayedMessage: argNotificationMessage.IsReplayedMessage,
                getTagsCall: GetTagsCallStatus.Skipped,
                tags: argNotificationMessage.Tags,
                resourceSystemProperties: argNotificationMessage.ResourceSystemProperties,
                internalNotificationSource: InternalNotificationSource.Shredder);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Intentional. Config update handler should not throw.")]
        private static void UpdateConfig(string newConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabledString,
            string newConfigDictionarySnapshotResourceTypeToAllowedTenantIdsString,
            string newConfigDictionarySnapshotResourceTypeToProcessingTimeString,
            string newConfigDictionarySnapshotResourceTypeToARNSchemaVersionString,
            bool newGenerateBlobForPartitionFileMetadata)
        {
            var methodMonitor = ShredderProcessingJobUpdateConfig.ToMonitor();
            methodMonitor.OnStart();
            var previousConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled = ConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled;
            var previousConfigDictionarySnapshotResourceTypeToAllowedTenantIds = ConfigDictionarySnapshotResourceTypeToAllowedTenantIds;
            var previousConfigDictionarySnapshotResourceTypeToProcessingTime = ConfigDictionarySnapshotResourceTypeToProcessingTime;
            var previousConfigDictionarySnapshotResourceTypeToARNSchemaVersion = ConfigDictionarySnapshotResourceTypeToARNSchemaVersion;
            var previousGenerateBlobForPartitionFileMetadata = GenerateBlobForPartitionFileMetadata;
            try
            {
                var newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabledString)
                       .ToDictionary(kv => kv.Key, kv => bool.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToAllowedTenantIds = ConfigurationUtils.ParseDictionaryOfListFromString<string>(newConfigDictionarySnapshotResourceTypeToAllowedTenantIdsString);
                var newConfigDictionarySnapshotResourceTypeToProcessingTime = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToProcessingTimeString)
                       .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToARNSchemaVersion = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToARNSchemaVersionString)
                       .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);

                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled"] = newConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabledString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToAllowedTenantIds"] = newConfigDictionarySnapshotResourceTypeToAllowedTenantIdsString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToProcessingTime"] = newConfigDictionarySnapshotResourceTypeToProcessingTimeString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToARNSchemaVersion"] = newConfigDictionarySnapshotResourceTypeToARNSchemaVersionString;
                methodMonitor.Activity.Properties["NewGenerateBlobForPartitionFileMetadata"] = newGenerateBlobForPartitionFileMetadata;

                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled"] = previousConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToAllowedTenantIds"] = previousConfigDictionarySnapshotResourceTypeToAllowedTenantIds != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionaryOfList<string, string>(previousConfigDictionarySnapshotResourceTypeToAllowedTenantIds) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToProcessingTime"] = previousConfigDictionarySnapshotResourceTypeToProcessingTime != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToProcessingTime) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToARNSchemaVersion"] = previousConfigDictionarySnapshotResourceTypeToARNSchemaVersion != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToARNSchemaVersion) : string.Empty;
                methodMonitor.Activity.Properties["PreviousGenerateBlobForPartitionFileMetadata"] = previousGenerateBlobForPartitionFileMetadata;

                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled, newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled);
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToAllowedTenantIds, newConfigDictionarySnapshotResourceTypeToAllowedTenantIds);
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToProcessingTime, newConfigDictionarySnapshotResourceTypeToProcessingTime); 
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToARNSchemaVersion, newConfigDictionarySnapshotResourceTypeToARNSchemaVersion);
                GenerateBlobForPartitionFileMetadata = newGenerateBlobForPartitionFileMetadata;

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                // NOTE: This should never trigger, but it's here just in case
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled, previousConfigDictionarySnapshotResourceTypeToShredderBigProcessingJobEnabled);
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToAllowedTenantIds, previousConfigDictionarySnapshotResourceTypeToAllowedTenantIds);
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToProcessingTime, previousConfigDictionarySnapshotResourceTypeToProcessingTime);
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToARNSchemaVersion, previousConfigDictionarySnapshotResourceTypeToARNSchemaVersion);
                GenerateBlobForPartitionFileMetadata = previousGenerateBlobForPartitionFileMetadata;
                methodMonitor.OnError(ex);
            }
        }
    }
}

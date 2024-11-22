namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileAwaitCopyJobs
{
    using global::Azure.Storage.Blobs.Models;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.AzureStorage;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Jobs.Base;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Configuration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Exceptions;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.TypeSpace.Contracts.ResourceProviderManifest;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// ShredderBigFileAwaitCopyJob
    /// </summary>
    internal class ShredderBigFileAwaitCopyJob : PassthroughJob<ShredderBigFileAwaitCopyJobContext>
    {
        #region Tracing

        private static readonly ActivityMonitorFactory ShredderBigFileAwaitCopyJobExecuteInternalPassthroughAsync =
            new ActivityMonitorFactory("ShredderBigFileAwaitCopyJob.ExecuteInternalPassthroughAsync");

        private static readonly ActivityMonitorFactory ShredderBigFileAwaitCopyJobUpdateConfig =
            new ActivityMonitorFactory("ShredderBigFileAwaitCopyJob.UpdateConfig");

        #endregion

        #region Private Members

        private static Dictionary<string, string> configDictionarySnapshotResourceTypeToSnapshotName;

        private static Dictionary<string, int> configDictionarySnapshotResourceTypeToKustoPartitions;

        private static Dictionary<string, long> configDictionarySnapshotResourceTypeToFilePartitionSizeInBytes;

        private static Dictionary<string, string> configDictionarySnapshotResourceTypeToNotificationResourceType;

        private static Dictionary<string, string> configDictionarySnapshotResourceTypeToScopeId;

        private static Dictionary<string, ResourceProviderRoutingType> configDictionarySnapshotResourceTypeToRoutingType;

        #endregion

        #region Constructors

        static ShredderBigFileAwaitCopyJob()
        {
            ServiceConfiguration.Current.Requirement
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToSnapshotName)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToKustoPartitions)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToFilePartitionSizeInMB)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToNotificationResourceType)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToScopeId)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToRoutingType)
                .RegisterCallbackAndInitialize(UpdateConfig);
        }

        public ShredderBigFileAwaitCopyJob()
        {
        }

        #endregion

        #region Protected Methods

        protected override async Task ExecuteInternalPassthroughAsync(ShredderBigFileAwaitCopyJobContext context, CancellationToken cancellationToken, IActivity parentActivity)
        {
            GuardHelper.ArgumentNotNull(context);
            GuardHelper.ArgumentNotNull(context.QueueMessage);
            GuardHelper.ArgumentNotNull(context.Message);
            GuardHelper.ArgumentNotNull(context.Message.NotificationMessage);

            var methodMonitor = ShredderBigFileAwaitCopyJobExecuteInternalPassthroughAsync.ToMonitor(parentActivity);
            methodMonitor.Activity["MessageId"] = context.QueueMessage.MessageId;
            methodMonitor.Activity["PopReceipt"] = context.QueueMessage.PopReceipt;
            methodMonitor.Activity["DequeueCount"] = context.QueueMessage.DequeueCount;
            methodMonitor.Activity["InsertionTime"] = context.QueueMessage.InsertionTime;
            methodMonitor.Activity["NotificationSnapshotTime"] = context.Message.NotificationMessage.Timestamp;
            methodMonitor.Activity["PartnerBlobUri"] = BlobLoggingUtils.HideSigFromBlobUri(context.Message.NotificationMessage.DataUri);
            methodMonitor.Activity["CorrelationId"] = context.Message.CorrelationId;

            methodMonitor.OnStart();
            try
            {
                var snapshotContainerPrefix = ConfigurationConstants.ShredderSnapshotContainerPrefix;
                var resourceType = context.Message.NotificationMessage.ResourceType;
                var snapshotContainerName = $"{snapshotContainerPrefix}{configDictionarySnapshotResourceTypeToSnapshotName[resourceType]}-{context.Message.NotificationMessage.Timestamp.ToString(ConfigurationConstants.ShredderSnapshotContainerSuffixFormat)}";
                var uploadContainer = context.BlobClient.GetContainerReference(snapshotContainerName);
                await uploadContainer.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
                var blobUri = new Uri(context.Message.NotificationMessage.DataUri);
                var blobUriFileName = Path.GetFileName(blobUri.LocalPath);
                var blobUriFileNameWithoutExtension = Path.GetFileNameWithoutExtension(blobUri.LocalPath);
                var snapshotBlobPath = $"{blobUriFileNameWithoutExtension}/{blobUriFileName}";

                var allSnapshotMessagesQueued = uploadContainer.GetBlockBlobReference($"{blobUriFileNameWithoutExtension}/{ConfigurationConstants.AllSnapshotMessagesQueued}");

                if (await allSnapshotMessagesQueued.ExistsAsync(cancellationToken).IgnoreContext())
                {
                    methodMonitor.Activity["AllSnapshotMessagesQueued"] = bool.TrueString;
                    methodMonitor.OnCompleted();
                    return;
                }

                // Use snapshotCopyCompleted to allow manual copies.
                var snapshotCopyCompletedBlob = uploadContainer.GetBlockBlobReference($"{blobUriFileNameWithoutExtension}/{ConfigurationConstants.SnapshotCopyCompleted}");
                var snapshotBlob = uploadContainer.GetBlockBlobReference(snapshotBlobPath);
                if (await snapshotCopyCompletedBlob.ExistsAsync(cancellationToken).IgnoreContext())
                {
                    methodMonitor.Activity["SnapshotCopyCompleted"] = bool.TrueString;
                    await snapshotBlob.FetchAttributesAsync(cancellationToken).IgnoreContext();
                }
                else if (await snapshotBlob.ExistsAsync(cancellationToken).IgnoreContext())
                {
                    methodMonitor.Activity["SnapshotBlobExists"] = bool.TrueString;
                    await snapshotBlob.FetchAttributesAsync(cancellationToken).IgnoreContext();
                    await snapshotCopyCompletedBlob.UploadFromStreamAsync(new MemoryStream(), cancellationToken).IgnoreContext();
                }
                else
                {
                    IReadOnlyList<BlobBlock> uncommitedBlocks;
                    try
                    {
                        uncommitedBlocks = await snapshotBlob.ListUncommitedBlocksAsync(cancellationToken).IgnoreContext();
                    }
                    catch (Exception ex) when (ex.IsAzureBlobNotFound())
                    {
                        methodMonitor.Activity["BlobNotFound"] = bool.TrueString;
                        methodMonitor.OnCompleted();
                        throw new ShredderSnapshotCopyNotCompleteException("No blocks have been put yet.");
                    }
                    methodMonitor.Activity["UncommittedBlockCount"] = uncommitedBlocks.Count;
                    if (uncommitedBlocks.Count < context.Message.PartnerBlockCount)
                    {
                        methodMonitor.OnCompleted();
                        throw new ShredderSnapshotCopyNotCompleteException("Not all blocks have been put yet.");
                    }

                    var partnerBlockBlob = context.GetPartnerCloudBlockBlob();
                    var partnerCommitedBlocks = await partnerBlockBlob.ListCommitedBlocksAsync(cancellationToken).IgnoreContext();
                    await snapshotBlob.PutBlockListAsync(partnerCommitedBlocks.Select(block => block.Name), cancellationToken).IgnoreContext();
                    await snapshotBlob.FetchAttributesAsync(cancellationToken).IgnoreContext();
                    await snapshotCopyCompletedBlob.UploadFromStreamAsync(new MemoryStream(), cancellationToken).IgnoreContext();
                }

                methodMonitor.Activity["StartQueueMessages"] = bool.TrueString;
                await QueueSplitMessagesAsync(context,
                        snapshotContainerName, blobUriFileNameWithoutExtension, snapshotBlobPath,
                        snapshotBlob.Properties.Length, cancellationToken, methodMonitor.Activity).IgnoreContext();
                await allSnapshotMessagesQueued.UploadFromStreamAsync(new MemoryStream(), cancellationToken).IgnoreContext();

                methodMonitor.OnCompleted();
            }
            catch (Exception e)
            {
                if (!(e is ShredderSnapshotCopyNotCompleteException))
                {
                    methodMonitor.OnError(e);
                }
                throw;
            }
        }

        #endregion

        #region Private Methods

        private async Task QueueSplitMessagesAsync(ShredderBigFileAwaitCopyJobContext context,
            string snapshotContainerName, string snapshotBlobFolder, string snapshotBlobPath,
            long blobLength, CancellationToken cancellationToken, IActivity parentActivity)
        {
            var localGoalSegmentSizeInBytes = configDictionarySnapshotResourceTypeToFilePartitionSizeInBytes[context.Message.NotificationMessage.ResourceType];
            var totalSegments = (int)((blobLength + localGoalSegmentSizeInBytes - 1) / localGoalSegmentSizeInBytes);
            for (var segmentIndex = 0; segmentIndex < totalSegments; segmentIndex++)
            {
                var message = new QueueMessage<ShredderProcessingMessage>(
                                       new ShredderProcessingMessage(
                                           context.Message.NotificationMessage,
                                           snapshotContainerName,
                                           snapshotBlobFolder,
                                           snapshotBlobPath,
                                           context.Message.NotificationMessage.DataUri,
                                           context.Message.NotificationMessage.Timestamp,
                                           segmentIndex,
                                           totalSegments,
                                           configDictionarySnapshotResourceTypeToKustoPartitions[context.Message.NotificationMessage.ResourceType],
                                           configDictionarySnapshotResourceTypeToNotificationResourceType[context.Message.NotificationMessage.ResourceType],
                                           configDictionarySnapshotResourceTypeToScopeId[context.Message.NotificationMessage.ResourceType],
                                           configDictionarySnapshotResourceTypeToRoutingType[context.Message.NotificationMessage.ResourceType],
                                           parentActivity.Context));

                await context.ShredderProcessingQueue.AddMessageAsync(message,
                                                            cancellationToken,
                                                            TimeSpan.FromDays(ServiceConfiguration.Current.Get<double>(
                                                                        ServiceConfigConstants.ShredderSection,
                                                                        ConfigurationConstants.ShredderProcessingQueueMessageDaysToLive)),
                                                            TimeSpan.Zero)
                    .IgnoreContext();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Intentional. Config update handler should not throw.")]
        private static void UpdateConfig(string newConfigDictionarySnapshotResourceTypeToSnapshotNameString,
            string newConfigDictionarySnapshotResourceTypeToKustoPartitionsString,
            string newConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytesString,
            string newConfigDictionarySnapshotResourceTypeToNotificationResourceTypeString,
            string newConfigDictionarySnapshotResourceTypeToScopeIdString,
            string newConfigDictionarySnapshotResourceTypeToRoutingTypeString)
        {
            var methodMonitor = ShredderBigFileAwaitCopyJobUpdateConfig.ToMonitor();
            methodMonitor.OnStart();
            var previousConfigDictionarySnapshotResourceTypeToSnapshotName = configDictionarySnapshotResourceTypeToSnapshotName;
            var previousConfigDictionarySnapshotResourceTypeToKustoPartitions = configDictionarySnapshotResourceTypeToKustoPartitions;
            var previousConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes = configDictionarySnapshotResourceTypeToFilePartitionSizeInBytes;
            var previousConfigDictionarySnapshotResourceTypeToNotificationResourceType = configDictionarySnapshotResourceTypeToNotificationResourceType;
            var previousConfigDictionarySnapshotResourceTypeToScopeId = configDictionarySnapshotResourceTypeToScopeId;
            var previousConfigDictionarySnapshotResourceTypeToRoutingType = configDictionarySnapshotResourceTypeToRoutingType;

            try
            {
                var newConfigDictionarySnapshotResourceTypeToSnapshotName = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToSnapshotNameString)
                       .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToKustoPartitions = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToKustoPartitionsString)
                   .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytesString)
                   .ToDictionary(kv => kv.Key, kv => long.Parse(kv.Value) * 1024 * 1024, StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToNotificationResourceType = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToNotificationResourceTypeString)
                       .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToScopeId = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToScopeIdString)
                      .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToRoutingType = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToRoutingTypeString)
                      .ToDictionary(kv => kv.Key, kv => (ResourceProviderRoutingType)int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);

                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToSnapshotName"] = newConfigDictionarySnapshotResourceTypeToSnapshotNameString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToKustoPartitions"] = newConfigDictionarySnapshotResourceTypeToKustoPartitionsString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes"] = newConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytesString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToNotificationResourceType"] = newConfigDictionarySnapshotResourceTypeToNotificationResourceTypeString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToScopeId"] = newConfigDictionarySnapshotResourceTypeToScopeIdString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToRoutingType"] = newConfigDictionarySnapshotResourceTypeToRoutingTypeString;

                methodMonitor.Activity.Properties["PreviousConfigDictionaryTargetRbacAggregationJobEnabled"] = previousConfigDictionarySnapshotResourceTypeToSnapshotName != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToSnapshotName) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionaryTargetRbacAggregationJobTimeout"] = previousConfigDictionarySnapshotResourceTypeToKustoPartitions != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToKustoPartitions) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes"] = previousConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes != null ?
                                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToNotificationResourceType"] = previousConfigDictionarySnapshotResourceTypeToNotificationResourceType != null ?
                                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToNotificationResourceType) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToScopeId"] = previousConfigDictionarySnapshotResourceTypeToScopeId != null ?
                                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToScopeId) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToRoutingType"] = previousConfigDictionarySnapshotResourceTypeToRoutingType != null ?
                                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToRoutingType) : string.Empty;
                
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToSnapshotName, newConfigDictionarySnapshotResourceTypeToSnapshotName);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToKustoPartitions, newConfigDictionarySnapshotResourceTypeToKustoPartitions);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToFilePartitionSizeInBytes, newConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToNotificationResourceType, newConfigDictionarySnapshotResourceTypeToNotificationResourceType);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToScopeId, newConfigDictionarySnapshotResourceTypeToScopeId);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToRoutingType, newConfigDictionarySnapshotResourceTypeToRoutingType);

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                // NOTE: This should never trigger, but it's here just in case
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToSnapshotName, previousConfigDictionarySnapshotResourceTypeToSnapshotName);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToKustoPartitions, previousConfigDictionarySnapshotResourceTypeToKustoPartitions);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToFilePartitionSizeInBytes, previousConfigDictionarySnapshotResourceTypeToFilePartitionSizeInBytes);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToNotificationResourceType, previousConfigDictionarySnapshotResourceTypeToNotificationResourceType);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToScopeId, previousConfigDictionarySnapshotResourceTypeToScopeId);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToRoutingType, previousConfigDictionarySnapshotResourceTypeToRoutingType);

                methodMonitor.OnError(ex);
            }
        }

        #endregion

    }
}

namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileStartCopyJobs
{
    using global::Azure.Storage.Blobs.Models;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Jobs.Base;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceMonitoring.Ifx;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy.ErrorStrategy;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Utils;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities;

    /// <summary>
    /// ShredderBigFileStartCopyJob
    /// </summary>
    internal class ShredderBigFileStartCopyJob : PassthroughJob<ShredderBigFileStartCopyJobContext>
    {
        #region Tracing

        private static readonly ActivityMonitorFactory ShredderBigFileStartCopyJobExecuteInternalPassthroughAsync =
            new ActivityMonitorFactory("ShredderBigFileStartCopyJob.ExecuteInternalPassthroughAsync");

        private static readonly ActivityMonitorFactory ShredderBigFileStartCopyJobUpdateConfig =
            new ActivityMonitorFactory("ShredderBigFileStartCopyJob.UpdateConfig");

        #endregion

        #region Constants

        protected static readonly IRetryPolicy RetryPolicy = new RetryPolicy(new CatchAllErrorStrategy(), 3, TimeSpan.FromSeconds(5));

        #endregion

        #region Private Members

        private static Dictionary<string, string> configDictionarySnapshotResourceTypeToSnapshotName;

        private static Dictionary<string, bool> configDictionarySnapshotResourceTypeToShredderBigFileJobEnabled;

        private static Dictionary<string, int> configDictionarySnapshotResourceTypeToBlocksToCopyPerMessage;

        private static Dictionary<string, int> configDictionarySnapshotResourceTypeToQueueBatchLimit;

        private static Dictionary<string, int> configDictionarySnapshotResourceTypeToCopyTime;

        #endregion

        #region Constructors

        static ShredderBigFileStartCopyJob()
        {
            ServiceConfiguration.Current.Requirement
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToSnapshotName)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToBlocksToCopyPerMessage)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToQueueBatchLimit)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToShredderBigFileJobEnabled)
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToCopyTime)
                .RegisterCallbackAndInitialize(UpdateConfig);
        }

        public ShredderBigFileStartCopyJob()
        {
        }

        #endregion

        #region Protected Methods

        protected override async Task ExecuteInternalPassthroughAsync(ShredderBigFileStartCopyJobContext context, CancellationToken cancellationToken, IActivity parentActivity)
        {
            GuardHelper.ArgumentNotNull(context);
            GuardHelper.ArgumentNotNull(context.QueueMessage);
            GuardHelper.ArgumentNotNull(context.Message);

            var methodMonitor = ShredderBigFileStartCopyJobExecuteInternalPassthroughAsync.ToMonitor(parentActivity);
            methodMonitor.Activity["MessageId"] = context.QueueMessage.MessageId;
            methodMonitor.Activity["PopReceipt"] = context.QueueMessage.PopReceipt;
            methodMonitor.Activity["DequeueCount"] = context.QueueMessage.DequeueCount;
            methodMonitor.Activity["InsertionTime"] = context.QueueMessage.InsertionTime;
            methodMonitor.Activity["NotificationSnapshotTime"] = context.Message.Timestamp;
            methodMonitor.Activity["PartnerBlobUri"] = BlobLoggingUtils.HideSigFromBlobUri(context.Message.DataUri);
            methodMonitor.Activity["CorrelationId"] = context.Message.CorrelationId;

            methodMonitor.OnStart();
            try
            {
                var snapshotContainerPrefix = ConfigurationConstants.ShredderSnapshotContainerPrefix;
                var resourceType = context.Message.ResourceType;
                var snapshotContainerName = $"{snapshotContainerPrefix}{configDictionarySnapshotResourceTypeToSnapshotName[resourceType]}-{context.Message.Timestamp.ToString(ConfigurationConstants.ShredderSnapshotContainerSuffixFormat)}";
                var uploadContainer = context.BlobClient.GetContainerReference(snapshotContainerName);
                await uploadContainer.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
                var blobUri = new Uri(context.Message.DataUri);
                var blobUriFileName = Path.GetFileName(blobUri.LocalPath);
                var blobUriFileNameWithoutExtension = Path.GetFileNameWithoutExtension(blobUri.LocalPath);
                var snapshotBlobPath = $"{blobUriFileNameWithoutExtension}/{blobUriFileName}";

                var allSnapshotBlockCopyMessagesQueued = uploadContainer.GetBlockBlobReference($"{blobUriFileNameWithoutExtension}/{ConfigurationConstants.AllSnapshotBlockCopyMessagesQueued}");

                if (!configDictionarySnapshotResourceTypeToShredderBigFileJobEnabled[resourceType])
                {
                    methodMonitor.Activity["JobDisabled"] = bool.TrueString;
                    methodMonitor.OnCompleted();
                    return;
                }

                if (await allSnapshotBlockCopyMessagesQueued.ExistsAsync(cancellationToken).IgnoreContext())
                {
                    methodMonitor.Activity["SnapshotBlockCopyMessagesQueued"] = bool.TrueString;
                    methodMonitor.OnCompleted();
                    return;
                }

                methodMonitor.Activity["StartQueueMessages"] = bool.TrueString;
                var partnerBlockBlob = context.GetPartnerCloudBlockBlob();
                var partnerCommitedBlocks = await partnerBlockBlob.ListCommitedBlocksAsync(cancellationToken).IgnoreContext();
                methodMonitor.Activity["PartnerBlockCount"] = partnerCommitedBlocks.Count;
                await QueueSplitMessagesAsync(context, resourceType, snapshotContainerName, snapshotBlobPath,
                    partnerCommitedBlocks, cancellationToken, methodMonitor.Activity).IgnoreContext();
                await allSnapshotBlockCopyMessagesQueued.UploadFromStreamAsync(new MemoryStream(), cancellationToken).IgnoreContext();

                var dimensions = new[] { "DataType", configDictionarySnapshotResourceTypeToSnapshotName[resourceType]};
                IfxMetricLogger.LogMetricValue(
                    IfxTracingConstants.ShredderSnapshotOriginalFileSizeMetric,
                    partnerCommitedBlocks.Sum(block => block.SizeLong),
                    parentActivity: methodMonitor.Activity,
                    metricDimensionNamesValues: dimensions);

                methodMonitor.OnCompleted();
            }
            catch (Exception e)
            {
                methodMonitor.OnError(e);
                throw;
            }
        }

        #endregion

        #region Private Methods

        private async Task QueueSplitMessagesAsync(ShredderBigFileStartCopyJobContext context, string resourceType,
            string containerName, string snapshotBlobPath,
            IReadOnlyList<BlobBlock> partnerCommitedBlocks, CancellationToken cancellationToken, IActivity parentActivity)
        {
            // Save local in case of hot config.
            var localBlocksToCopyPerMessage = configDictionarySnapshotResourceTypeToBlocksToCopyPerMessage[resourceType];
            var localQueueBatches = configDictionarySnapshotResourceTypeToQueueBatchLimit[resourceType];
            var totalMessages = (partnerCommitedBlocks.Count + localBlocksToCopyPerMessage - 1) / localBlocksToCopyPerMessage;
            // Find how many batches there will be.
            var batchCount = (totalMessages + localQueueBatches - 1) / localQueueBatches;
            var blockOffsetStart = 0L;

            (var delta, var numberOfMessagesInGroup) = ShredderUtils.GetParametersToQueueMessagesOverPeriodOfTime(configDictionarySnapshotResourceTypeToCopyTime[resourceType], batchCount + 1);

            parentActivity["MaximumTimeToCopy"] = configDictionarySnapshotResourceTypeToCopyTime[resourceType];
            parentActivity["TotalMessagesForTheSnapshotJob"] = batchCount + 1;
            parentActivity["Delta"] = delta;
            parentActivity["NumberOfMessagesInGroup"] = numberOfMessagesInGroup;

            for (var batchIndex = 0; batchIndex < batchCount; batchIndex++)
            {
                var messageOffset = batchIndex * localQueueBatches;
                var maxMessageNumberInBatch = Math.Min(totalMessages, (batchIndex + 1) * localQueueBatches) - messageOffset - 1;
                var tasks = new Task[maxMessageNumberInBatch + 1];
                for (var messageNumberInBatch = 0; messageNumberInBatch <= maxMessageNumberInBatch; messageNumberInBatch++)
                {
                    var blockIndex = (messageNumberInBatch + messageOffset) * localBlocksToCopyPerMessage;
                    var maxBlockIndexInMessage = Math.Min(partnerCommitedBlocks.Count, (messageNumberInBatch + messageOffset + 1) * localBlocksToCopyPerMessage) - 1;
                    var blockCopyInfos = new List<BlockCopyInfo>(maxBlockIndexInMessage - blockIndex + 1);
                    for (; blockIndex <= maxBlockIndexInMessage; blockIndex++)
                    {
                        var partnerCommitedBlock = partnerCommitedBlocks[blockIndex];
                        blockCopyInfos.Add(new BlockCopyInfo(partnerCommitedBlock.Name, blockOffsetStart, partnerCommitedBlock.SizeLong));
                        blockOffsetStart += partnerCommitedBlock.SizeLong;
                    }
                    var message = new QueueMessage<ShredderBigFileBlockCopyMessage>(
                        new ShredderBigFileBlockCopyMessage(context.Message.DataUri,
                            context.Message.Timestamp, blockCopyInfos, parentActivity.Context,
                            resourceType, containerName, snapshotBlobPath));

                    var visibilityDelay = (batchIndex / numberOfMessagesInGroup) * delta;

                    tasks[messageNumberInBatch] = RetryPolicy.ExecuteAsync(() =>
                        context.ShredderBigFileBlockCopyQueue.AddMessageAsync(
                            message,
                            cancellationToken,
                            visibilityDelay: TimeSpan.FromSeconds(visibilityDelay)));
                }
                await Task.WhenAll(tasks).IgnoreContext();
            }

            var awaitBlobCopyMessage = new QueueMessage<ShredderBigFileAwaitCopyMessage>(
                new ShredderBigFileAwaitCopyMessage(context.Message, partnerCommitedBlocks.Count, parentActivity.Context));
            await context.ShredderBigFileAwaitCopyQueue.AddMessageAsync(awaitBlobCopyMessage, cancellationToken).IgnoreContext();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Intentional. Config update handler should not throw.")]
        private static void UpdateConfig(
            string newConfigDictionarySnapshotResourceTypeToSnapshotNameString,
            string newConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessageString,
            string newConfigDictionarySnapshotResourceTypeToQueueBatchLimitString,
            string newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabledString,
            string newConfigDictionarySnapshotResourceTypeToCopyTimeString)
        {
            var methodMonitor = ShredderBigFileStartCopyJobUpdateConfig.ToMonitor();
            methodMonitor.OnStart();
            var previousConfigDictionarySnapshotResourceTypeToSnapshotName = configDictionarySnapshotResourceTypeToSnapshotName;
            var previousConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage = configDictionarySnapshotResourceTypeToBlocksToCopyPerMessage;
            var previousConfigDictionarySnapshotResourceTypeToQueueBatchLimit = configDictionarySnapshotResourceTypeToQueueBatchLimit;
            var previousConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled = configDictionarySnapshotResourceTypeToShredderBigFileJobEnabled;
            var previousConfigDictionarySnapshotResourceTypeToCopyTime = configDictionarySnapshotResourceTypeToCopyTime;

            try
            {
                var newConfigDictionarySnapshotResourceTypeToSnapshotName = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToSnapshotNameString)
                       .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessageString)
                       .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToQueueBatchLimit = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToQueueBatchLimitString)
                       .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabledString)
                       .ToDictionary(kv => kv.Key, kv => bool.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);
                var newConfigDictionarySnapshotResourceTypeToCopyTime = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToCopyTimeString)
                       .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);

                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToSnapshotName"] = newConfigDictionarySnapshotResourceTypeToSnapshotNameString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage"] = newConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessageString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToQueueBatchLimit"] = newConfigDictionarySnapshotResourceTypeToQueueBatchLimitString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled"] = newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabledString; methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled"] = newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabledString;
                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToCopyTime"] = newConfigDictionarySnapshotResourceTypeToCopyTimeString;

                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToSnapshotName"] = previousConfigDictionarySnapshotResourceTypeToSnapshotName != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToSnapshotName) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage"] = previousConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToSnapshotName) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToQueueBatchLimit"] = previousConfigDictionarySnapshotResourceTypeToQueueBatchLimit != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToQueueBatchLimit) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled"] = previousConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled) : string.Empty;
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToCopyTime"] = previousConfigDictionarySnapshotResourceTypeToCopyTime != null ?
                   ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToCopyTime) : string.Empty;

                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToSnapshotName, newConfigDictionarySnapshotResourceTypeToSnapshotName);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToBlocksToCopyPerMessage, newConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToQueueBatchLimit, newConfigDictionarySnapshotResourceTypeToQueueBatchLimit);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToShredderBigFileJobEnabled, newConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToCopyTime, newConfigDictionarySnapshotResourceTypeToCopyTime);

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                // NOTE: This should never trigger, but it's here just in case
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToSnapshotName, previousConfigDictionarySnapshotResourceTypeToSnapshotName);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToBlocksToCopyPerMessage, previousConfigDictionarySnapshotResourceTypeToBlocksToCopyPerMessage);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToQueueBatchLimit, previousConfigDictionarySnapshotResourceTypeToQueueBatchLimit);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToShredderBigFileJobEnabled, previousConfigDictionarySnapshotResourceTypeToShredderBigFileJobEnabled);
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToCopyTime, previousConfigDictionarySnapshotResourceTypeToCopyTime);

                methodMonitor.OnError(ex);
            }
        }

        #endregion

    }
}

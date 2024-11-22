namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using System;
    using System.Diagnostics.CodeAnalysis;

    internal static class ConfigurationConstants
    {
        #region Constants

        public static readonly TimeSpan QueueMessageVisibilityTimeout = TimeSpan.Parse(
           ServiceConfiguration.Current.Get(ServiceConfigConstants.ShredderSection,
               ConfigurationConstants.QueueMessageVisibilityTimeoutEntry));

        public static readonly TimeSpan QueueMessageVisibilityCheckAhead = TimeSpan.Parse(
            ServiceConfiguration.Current.Get(
                ServiceConfigConstants.ShredderSection, ConfigurationConstants.QueueMessageVisibilityCheckAheadEntry));

        public const string SnapshotResourceTypeToSnapshotName = "SnapshotResourceTypeToSnapshotName";
        public const string SnapshotResourceTypeToKustoPartitions = "SnapshotResourceTypeToKustoPartitions";
        public const string SnapshotResourceTypeToFilePartitionSizeInMB = "SnapshotResourceTypeToFilePartitionSizeInMB";
        public const string SnapshotResourceTypeToNotificationResourceType = "SnapshotResourceTypeToNotificationResourceType";
        public const string SnapshotResourceTypeToScopeId = "SnapshotResourceTypeToScopeId";
        public const string SnapshotResourceTypeToRoutingType = "SnapshotResourceTypeToRoutingType";
        public const string SnapshotResourceTypeToShredderBigFileJobEnabled = "SnapshotResourceTypeToShredderBigFileJobEnabled";
        public const string SnapshotResourceTypeToShredderBigProcessingJobEnabled = "SnapshotResourceTypeToShredderBigProcessingJobEnabled";
        public const string SnapshotResourceTypeToAllowedTenantIds = "SnapshotResourceTypeToAllowedTenantIds";
        public const string SnapshotResourceTypeToBlocksToCopyPerMessage = "SnapshotResourceTypeToBlocksToCopyPerMessage";
        public const string SnapshotResourceTypeToQueueBatchLimit = "SnapshotResourceTypeToQueueBatchLimit";
        public const string SnapshotResourceTypeToBlockCopyTimeout = "SnapshotResourceTypeToBlockCopyTimeout";
        public const string SnapshotResourceTypeToCopyTime = "SnapshotResourceTypeToCopyTime";
        public const string SnapshotResourceTypeToProcessingTime = "SnapshotResourceTypeToProcessingTime";
        public const string SnapshotResourceTypeToShredderStorageAccount = "SnapshotResourceTypeToShredderStorageAccount";
        public const string SnapshotResourceTypeToARNSchemaVersion = "SnapshotResourceTypeToARNSchemaVersion";

        public const string QueueMessageVisibilityTimeoutEntry = "QueueMessageVisibilityTimeout";
        public const string QueueMessageVisibilityCheckAheadEntry = "QueueMessageVisibilityCheckAhead";

        public const string ShredderOptedOutActivities = "ShredderOptedOutActivities";
        public const string UnskippableStartedEventsForLogs = "UnskippableStartedEventsForLogs";
        public const string IsActivityStartedDisabled = "IsActivityStartedDisabled";

        public const string ShredderServiceBigFileQueuePollingPeriod = "ShredderServiceBigFileQueuePollingPeriod";
        public const string ShredderBigFileQueue = "ShredderBigFileQueue";
        public const string ShredderBigFileFailureQueue = "ShredderBigFileFailureQueue";

        public const string ShredderBigFileJobManagerThreadPoolThreadLimitEntry = "ShredderBigFileJobManagerThreadPoolThreadLimit";
        public const string ShredderBigFileJobManagerMaxQueueMessageDequeueCountEntry = "ShredderBigFileJobManagerMaxQueueMessageDequeueCount";
        public const string ShredderBigFileJobManagerQueueMessageMaxVisibilityTimeoutEntry = "ShredderBigFileJobManagerQueueMessageMaxVisibilityTimeout";
        public const string ShredderBigFileJobManagerDeltaQueueMessageVisibilityTimeoutEntry = "ShredderBigFileJobManagerDeltaQueueMessageVisibilityTimeout";

        public const string ShredderBigFileBlockCopyQueuePollingPeriod = "ShredderBigFileBlockCopyQueuePollingPeriod";
        public const string ShredderBigFileBlockCopyQueueMessageDaysToLive = "ShredderBigFileBlockCopyQueueMessageDaysToLive";
        public const string ShredderBigFileBlockCopyQueue = "ShredderBigFileBlockCopyQueue";
        public const string ShredderBigFileBlockCopyFailureQueue = "ShredderBigFileBlockCopyFailureQueue";

        public const string ShredderBigFileBlockCopyJobManagerThreadPoolThreadLimitEntry = "ShredderBigFileBlockCopyJobManagerThreadPoolThreadLimit";
        public const string ShredderBigFileBlockCopyJobManagerMaxQueueMessageDequeueCountEntry = "ShredderBigFileBlockCopyJobManagerMaxQueueMessageDequeueCount";
        public const string ShredderBigFileBlockCopyJobManagerQueueMessageMaxVisibilityTimeoutEntry = "ShredderBigFileBlockCopyJobManagerQueueMessageMaxVisibilityTimeout";
        public const string ShredderBigFileBlockCopyJobManagerDeltaQueueMessageVisibilityTimeoutEntry = "ShredderBigFileBlockCopyJobManagerDeltaQueueMessageVisibilityTimeout";

        public const string ShredderBigFileAwaitCopyQueuePollingPeriod = "ShredderBigFileAwaitCopyQueuePollingPeriod";
        public const string ShredderBigFileAwaitCopyQueueMessageDaysToLive = "ShredderBigFileAwaitCopyQueueMessageDaysToLive";
        public const string ShredderBigFileAwaitCopyQueue = "ShredderBigFileAwaitCopyQueue";
        public const string ShredderBigFileAwaitCopyFailureQueue = "ShredderBigFileAwaitCopyFailureQueue";

        public const string ShredderBigFileAwaitCopyJobManagerThreadPoolThreadLimitEntry = "ShredderBigFileAwaitCopyJobManagerThreadPoolThreadLimit";
        public const string ShredderBigFileAwaitCopyJobManagerMaxQueueMessageDequeueCountEntry = "ShredderBigFileAwaitCopyJobManagerMaxQueueMessageDequeueCount";
        public const string ShredderBigFileAwaitCopyJobManagerQueueMessageMaxVisibilityTimeoutEntry = "ShredderBigFileAwaitCopyJobManagerQueueMessageMaxVisibilityTimeout";
        public const string ShredderBigFileAwaitCopyJobManagerDeltaQueueMessageVisibilityTimeoutEntry = "ShredderBigFileAwaitCopyJobManagerDeltaQueueMessageVisibilityTimeout";

        public const string ShredderServiceProcessingQueuePollingPeriod = "ShredderServiceProcessingQueuePollingPeriod";
        public const string ShredderProcessingQueueMessageDaysToLive = "ShredderProcessingQueueMessageDaysToLive";
        public const string ShredderProcessingQueue = "ShredderProcessingQueue";
        public const string ShredderProcessingFailureQueue = "ShredderProcessingFailureQueue";

        public const string ShredderProcessingJobManagerThreadPoolThreadLimitEntry = "ShredderProcessingJobManagerThreadPoolThreadLimit";
        public const string ShredderProcessingJobManagerMaxQueueMessageDequeueCountEntry = "ShredderProcessingJobManagerMaxQueueMessageDequeueCount";
        public const string ShredderProcessingJobManagerQueueMessageMaxVisibilityTimeoutEntry = "ShredderProcessingJobManagerQueueMessageMaxVisibilityTimeout";
        public const string ShredderProcessingJobManagerDeltaQueueMessageVisibilityTimeoutEntry = "ShredderProcessingJobManagerDeltaQueueMessageVisibilityTimeout";
        public const string GenerateBlobForPartitionFileMetadata = "GenerateBlobForPartitionFileMetadata";


        public const string ShredderSnapshotContainerPrefix = "shreddersnapshot-";

        [StringSyntax(StringSyntaxAttribute.DateTimeFormat)]
        public const string ShredderSnapshotContainerSuffixFormat = "yyyyMMddHH";

        public const string AllSnapshotMessagesQueued = "AllSnapshotMessagesQueued";

        public const string AllSnapshotBlockCopyMessagesQueued = "AllSnapshotBlockCopyMessagesQueued";

        public const string SnapshotCopyCompleted = "SnapshotCopyCompleted";

        public const string CompletedFileName = "Completed";

        public const string PartitionDataFileName = "PartitionData";

        public const string JsonFileType = ".json";

        #endregion
    }
}
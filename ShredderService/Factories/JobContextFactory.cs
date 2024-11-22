namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Factories
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts.Blob;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileAwaitCopyJobs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileBlockCopyJobs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileStartCopyJobs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderProcessingJobs;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;

    internal class JobContextFactory
    {
        #region Tracing

        private static readonly ActivityMonitorFactory JobContextFactoryJobUpdateConfig =
            new ActivityMonitorFactory("JobContextFactory.UpdateConfig");

        #endregion

        #region Constructors

        private JobContextFactory()
        {
        }

        static JobContextFactory()
        {
            ServiceConfiguration.Current.Requirement
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToShredderStorageAccount)
                .RegisterCallbackAndInitialize(UpdateConfig);
        }

        #endregion Constructors

        #region Singleton Impl

        public static JobContextFactory Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (SyncRoot)
                    {
                        if (_instance == null)
                        {
                            _instance = new JobContextFactory();
                        }
                    }
                }

                return _instance;
            }
        }

        private static volatile JobContextFactory _instance;

        private static readonly object SyncRoot = new object();

        private static Dictionary<string, int> ConfigDictionarySnapshotResourceTypeToShredderStorageAccount;

        #endregion Singleton Impl

        #region Public

        public ShredderBigFileStartCopyJobContext CreateShredderBigFileStartCopyJobContext(
            IQueue<ArgNotificationMessage> shredderBigFileStartCopyQueue,
            IQueue<ShredderBigFileBlockCopyMessage> shredderBigFileBlockCopyQueue,
            IQueue<ShredderBigFileAwaitCopyMessage> shredderBigFileAwaitCopyQueue,
            IQueueMessage<ArgNotificationMessage> queueMessage,
            IList<ICloudBlobClient> blobClients)
        {
            return new ShredderBigFileStartCopyJobContext(
                    shredderBigFileStartCopyQueue,
                    shredderBigFileBlockCopyQueue,
                    shredderBigFileAwaitCopyQueue,
                    queueMessage,
                    GetBlobClientForShredderSnapshotResourceFromShredderStorageAccount(
                        blobClients,
                        queueMessage.Message.ResourceType));
        }

        public ShredderBigFileBlockCopyJobContext CreateShredderBigFileBlockCopyJobContext(
            IQueue<ShredderBigFileBlockCopyMessage> shredderBigFileBlockCopyQueue,
            IQueueMessage<ShredderBigFileBlockCopyMessage> queueMessage,
            IList<ICloudBlobClient> blobClients)
        {
            return new ShredderBigFileBlockCopyJobContext(
                    shredderBigFileBlockCopyQueue,
                    queueMessage,
                    GetBlobClientForShredderSnapshotResourceFromShredderStorageAccount(
                        blobClients,
                        queueMessage.Message.ResourceType));
        }

        public ShredderBigFileAwaitCopyJobContext CreateShredderBigFileAwaitCopyJobContext(
            IQueue<ShredderBigFileAwaitCopyMessage> jobQueue,
            IQueue<ShredderProcessingMessage> shredderProcessingMessageQueue,
            IQueueMessage<ShredderBigFileAwaitCopyMessage> queueMessage,
            IList<ICloudBlobClient> blobClients)
        {
            return new ShredderBigFileAwaitCopyJobContext(
                    jobQueue,
                    shredderProcessingMessageQueue,
                    queueMessage,
                    GetBlobClientForShredderSnapshotResourceFromShredderStorageAccount(
                        blobClients,
                        queueMessage.Message.NotificationMessage.ResourceType));
        }

        public ShredderProcessingJobContext CreateShredderProcessingJobContext(
            IQueue<ShredderProcessingMessage> jobQueue,
            IQueue<ArgRetryNotificationMessage> snapshotNotificationRetryQueue,
            IQueueMessage<ShredderProcessingMessage> queueMessage,
            IList<ICloudBlobClient> blobClients)
        {
            return new ShredderProcessingJobContext(
                    jobQueue,
                    snapshotNotificationRetryQueue,
                    queueMessage,
                    GetBlobClientForShredderSnapshotResourceFromShredderStorageAccount(
                        blobClients,
                        queueMessage.Message.NotificationMessage.ResourceType));
        }

        public ICloudBlobClient GetBlobClientForShredderSnapshotResourceFromShredderStorageAccount(
            IList<ICloudBlobClient> blobClients,
            string resourceType)
        {
            return blobClients[ConfigDictionarySnapshotResourceTypeToShredderStorageAccount[resourceType]];
        }

        #endregion Public

        #region Private Methods

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Intentional. Config update handler should not throw.")]
        private static void UpdateConfig(string newConfigDictionarySnapshotResourceTypeToShredderStorageAccountString)
        {
            var methodMonitor = JobContextFactoryJobUpdateConfig.ToMonitor();
            methodMonitor.OnStart();
            var previousConfigDictionarySnapshotResourceTypeToShredderStorageAccount = ConfigDictionarySnapshotResourceTypeToShredderStorageAccount;

            try
            {
                var newConfigDictionarySnapshotResourceTypeToShredderStorageAccount = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToShredderStorageAccountString)
                       .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);

                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToShredderStorageAccount"] = newConfigDictionarySnapshotResourceTypeToShredderStorageAccountString;
                
                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToShredderStorageAccount"] = previousConfigDictionarySnapshotResourceTypeToShredderStorageAccount != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToShredderStorageAccount) : string.Empty;
                
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToShredderStorageAccount, newConfigDictionarySnapshotResourceTypeToShredderStorageAccount);

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                // NOTE: This should never trigger, but it's here just in case
                Interlocked.Exchange(ref ConfigDictionarySnapshotResourceTypeToShredderStorageAccount, previousConfigDictionarySnapshotResourceTypeToShredderStorageAccount);

                methodMonitor.OnError(ex);
            }
        }

        #endregion
    }
}
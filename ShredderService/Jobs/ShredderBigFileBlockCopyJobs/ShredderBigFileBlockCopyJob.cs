namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileBlockCopyJobs
{
    using global::Azure.Storage.Blobs.Models;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Jobs.Base;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Configuration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.TypeSpace.Contracts.ResourceProviderManifest;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// ShredderBigFileBlockCopyJob
    /// </summary>
    internal class ShredderBigFileBlockCopyJob : Job<ShredderBigFileBlockCopyJobContext, object, object>
    {
        private static readonly ActivityMonitorFactory ShredderBigFileBlockCopyJobUpdateConfig =
            new ActivityMonitorFactory("ShredderBigFileBlockCopyJob.UpdateConfig");
        private static readonly ActivityMonitorFactory ShredderBigFileBlockCopyJobExecuteInternalWithInputAsync =
            new ActivityMonitorFactory("ShredderBigFileBlockCopyJob.ExecuteInternalWithInputAsync");

        #region Private members

        private static Dictionary<string, int> configDictionarySnapshotResourceTypeToBlockCopyTimeout;


        #endregion

        static ShredderBigFileBlockCopyJob()
        {
            ServiceConfiguration.Current.Requirement
                .Add<string>(ServiceConfigConstants.ShredderSection, ConfigurationConstants.SnapshotResourceTypeToBlockCopyTimeout)
                .RegisterCallbackAndInitialize(UpdateConfig);
        }

        protected async override Task<object> ExecuteInternalWithInputAsync(ShredderBigFileBlockCopyJobContext context, object input, CancellationToken cancellationToken, IActivity parentActivity)
        {
            GuardHelper.ArgumentNotNull(context);
            GuardHelper.ArgumentNotNull(context.Message);
            var methodMonitor = ShredderBigFileBlockCopyJobExecuteInternalWithInputAsync.ToMonitor(parentActivity);
            methodMonitor.Activity["ResourceType"] = context.Message.ResourceType;
            methodMonitor.Activity["ContainerName"] = context.Message.ContainerName;
            methodMonitor.Activity["SnapshotBlobPath"] = context.Message.SnapshotBlobPath;
            methodMonitor.Activity["BlobUri"] = BlobLoggingUtils.HideSigFromBlobUri(context.Message.PartnerBlobUri);
            methodMonitor.Activity["BlockCopyInfosCount"] = context.Message.BlockCopyInfos.Count;
            methodMonitor.Activity["FirstBlockName"] = context.Message.BlockCopyInfos[0].Name;
            methodMonitor.Activity["FirstBlockOffset"] = context.Message.BlockCopyInfos[0].StartOffset;
            methodMonitor.Activity["NotificationSnapshotTime"] = context.Message.NotificationSnapshotTime;
            methodMonitor.OnStart();

            try
            {
                using (var cancellationTokenSource =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    await this.SnapshotBlockCopyHelperAsync(context,
                        methodMonitor.Activity, cancellationTokenSource.Token)
                        .WithTimeout(
                            TimeSpan.FromSeconds(configDictionarySnapshotResourceTypeToBlockCopyTimeout[context.Message.ResourceType]),
                            cancellationTokenSource)
                        .IgnoreContext();
                }
                methodMonitor.OnCompleted();
            }
            catch (Exception e)
            {
                methodMonitor.OnError(e);
                throw;
            }

            return true;
        }

        private async Task SnapshotBlockCopyHelperAsync(ShredderBigFileBlockCopyJobContext context, IActivity parentActivity, CancellationToken cancellationToken)
        {
            var copyContainer = context.BlobClient.GetContainerReference(context.Message.ContainerName);
            await copyContainer.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
            var snapshotBlob = copyContainer.GetBlockBlobReference(context.Message.SnapshotBlobPath);

            if (await snapshotBlob.ExistsAsync(cancellationToken).IgnoreContext())
            {
                parentActivity["SnapshotBlobExists"] = bool.TrueString;
                return;
            }

            var partnerBlob = context.GetPartnerCloudBlockBlob();
            await BlockCopyUtilities.CopyBlocksAsync(context.Message.BlockCopyInfos, partnerBlob, snapshotBlob, cancellationToken).IgnoreContext();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Intentional. Config update handler should not throw.")]
        private static void UpdateConfig(
            string newConfigDictionarySnapshotResourceTypeToBlockCopyTimeoutString)
        {
            var methodMonitor = ShredderBigFileBlockCopyJobUpdateConfig.ToMonitor();
            methodMonitor.OnStart();
            var previousConfigDictionarySnapshotResourceTypeToBlockCopyTimeout = configDictionarySnapshotResourceTypeToBlockCopyTimeout;

            try
            {
                var newConfigDictionarySnapshotResourceTypeToBlockCopyTimeout = ConfigurationUtils.ParseDictionaryFromString(newConfigDictionarySnapshotResourceTypeToBlockCopyTimeoutString)
                       .ToDictionary(kv => kv.Key, kv => int.Parse(kv.Value), StringComparer.OrdinalIgnoreCase);

                methodMonitor.Activity.Properties["NewConfigDictionarySnapshotResourceTypeToBlockCopyTimeout"] = newConfigDictionarySnapshotResourceTypeToBlockCopyTimeoutString;

                methodMonitor.Activity.Properties["PreviousConfigDictionarySnapshotResourceTypeToBlockCopyTimeout"] = previousConfigDictionarySnapshotResourceTypeToBlockCopyTimeout != null ?
                    ConfigurationUtils.ToSemicolonSeparatedDictionary(previousConfigDictionarySnapshotResourceTypeToBlockCopyTimeout) : string.Empty;
                
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToBlockCopyTimeout, newConfigDictionarySnapshotResourceTypeToBlockCopyTimeout);

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                // NOTE: This should never trigger, but it's here just in case
                Interlocked.Exchange(ref configDictionarySnapshotResourceTypeToBlockCopyTimeout, previousConfigDictionarySnapshotResourceTypeToBlockCopyTimeout);

                methodMonitor.OnError(ex);
            }
        }

    }
}

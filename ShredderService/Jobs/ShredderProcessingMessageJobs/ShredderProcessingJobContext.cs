namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderProcessingJobs
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts.Blob;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.JobContext;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;

    internal class ShredderProcessingJobContext : IQueueAwareJobContext<ShredderProcessingMessage>
    {
        #region Properties

        #region IQueueAwareJobContext

        public ShredderProcessingMessage Message => this.QueueMessage?.Message;

        public IQueue<ShredderProcessingMessage> Queue
        {
            get;
        }

        public IQueue<ArgRetryNotificationMessage> SnapshotNotificationRetryQueue
        {
            get;
        }

        public IQueueMessage<ShredderProcessingMessage> QueueMessage
        {
            get;
        }

        #endregion

        public ICloudBlobClient BlobClient
        {
            get;
        }

        #endregion

        #region Constructors

        public ShredderProcessingJobContext(
            IQueue<ShredderProcessingMessage> ShredderProcessingMessageQueue,
            IQueue<ArgRetryNotificationMessage> snapshotNotificationRetryQueue,
            IQueueMessage<ShredderProcessingMessage> ShredderProcessingMessage,
            ICloudBlobClient blobClient)
        {
            GuardHelper.ArgumentNotNull(ShredderProcessingMessageQueue);
            GuardHelper.ArgumentNotNull(snapshotNotificationRetryQueue);
            GuardHelper.ArgumentNotNull(ShredderProcessingMessage);
            GuardHelper.ArgumentNotNull(blobClient);

            this.Queue = ShredderProcessingMessageQueue;
            this.SnapshotNotificationRetryQueue = snapshotNotificationRetryQueue;
            this.QueueMessage = ShredderProcessingMessage;
            this.BlobClient = blobClient;
        }

        #endregion

        #region IEquatable Impl

        /// <summary>
        /// Equality comparison. Only returns true if 2 context objects refers to the same thing.
        /// </summary>
        /// <param name="other">The other.</param>
        /// <returns>If the contexts are equal.</returns>
        public bool Equals(IJobContext other)
        {
            return this == other;
        }

        #endregion
    }
}
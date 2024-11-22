namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileAwaitCopyJobs
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.AzureStorage.Blob;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts.Blob;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.JobContext;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using System;

    internal class ShredderBigFileAwaitCopyJobContext : IQueueAwareJobContext<ShredderBigFileAwaitCopyMessage>
    {
        #region Properties

        #region IQueueAwareJobContext

        public ShredderBigFileAwaitCopyMessage Message => this.QueueMessage?.Message;

        public IQueue<ShredderBigFileAwaitCopyMessage> Queue
        {
            get;
        }

        public IQueue<ShredderProcessingMessage> ShredderProcessingQueue
        {
            get;
        }

        public IQueueMessage<ShredderBigFileAwaitCopyMessage> QueueMessage
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

        public ShredderBigFileAwaitCopyJobContext(
            IQueue<ShredderBigFileAwaitCopyMessage> shredderBigFileAwaitCopyQueue,
            IQueue<ShredderProcessingMessage> shredderProcessingQueue,
            IQueueMessage<ShredderBigFileAwaitCopyMessage> shredderBigFileAwaitCopyMessage,
            ICloudBlobClient blobClient)
        {
            GuardHelper.ArgumentNotNull(shredderBigFileAwaitCopyQueue);
            GuardHelper.ArgumentNotNull(shredderProcessingQueue);
            GuardHelper.ArgumentNotNull(shredderBigFileAwaitCopyMessage);
            GuardHelper.ArgumentNotNull(blobClient);

            this.Queue = shredderBigFileAwaitCopyQueue;
            this.ShredderProcessingQueue = shredderProcessingQueue;
            this.QueueMessage = shredderBigFileAwaitCopyMessage;
            this.BlobClient = blobClient;
        }
        public virtual ICloudBlockBlob GetPartnerCloudBlockBlob()
        {
            return new AzureStorageCloudBlockBlob(new Uri(Message.NotificationMessage.DataUri));
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
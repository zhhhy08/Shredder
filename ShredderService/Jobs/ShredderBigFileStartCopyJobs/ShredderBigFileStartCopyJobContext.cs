namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileStartCopyJobs
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

    internal class ShredderBigFileStartCopyJobContext : IQueueAwareJobContext<ArgNotificationMessage>
    {
        #region Properties

        #region IQueueAwareJobContext

        public ArgNotificationMessage Message => this.QueueMessage?.Message;

        public IQueue<ArgNotificationMessage> Queue
        {
            get;
        }

        public IQueue<ShredderBigFileBlockCopyMessage> ShredderBigFileBlockCopyQueue
        {
            get;
        }

        public IQueue<ShredderBigFileAwaitCopyMessage> ShredderBigFileAwaitCopyQueue
        {
            get;
        }

        public IQueueMessage<ArgNotificationMessage> QueueMessage
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

        public ShredderBigFileStartCopyJobContext(
            IQueue<ArgNotificationMessage> shredderBigFileStartCopyQueue,
            IQueue<ShredderBigFileBlockCopyMessage> shredderBigFileBlockCopyQueue,
            IQueue<ShredderBigFileAwaitCopyMessage> shredderBigFileAwaitCopyQueue,
            IQueueMessage<ArgNotificationMessage> queueMessage,
            ICloudBlobClient blobClient)
        {
            GuardHelper.ArgumentNotNull(shredderBigFileStartCopyQueue);
            GuardHelper.ArgumentNotNull(shredderBigFileBlockCopyQueue);
            GuardHelper.ArgumentNotNull(shredderBigFileAwaitCopyQueue);
            GuardHelper.ArgumentNotNull(blobClient);

            this.Queue = shredderBigFileStartCopyQueue;
            this.ShredderBigFileBlockCopyQueue = shredderBigFileBlockCopyQueue;
            this.ShredderBigFileAwaitCopyQueue = shredderBigFileAwaitCopyQueue;
            this.QueueMessage = queueMessage;
            this.BlobClient = blobClient;
        }

        public virtual ICloudBlockBlob GetPartnerCloudBlockBlob()
        {
            return new AzureStorageCloudBlockBlob(new Uri(Message.DataUri));
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
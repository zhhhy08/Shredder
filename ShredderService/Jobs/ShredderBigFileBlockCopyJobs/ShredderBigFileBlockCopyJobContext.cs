﻿namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileBlockCopyJobs
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

    internal class ShredderBigFileBlockCopyJobContext : IQueueAwareJobContext<ShredderBigFileBlockCopyMessage>
    {
        #region Properties

        #region IQueueAwareJobContext

        public ShredderBigFileBlockCopyMessage Message => this.QueueMessage?.Message;

        public IQueue<ShredderBigFileBlockCopyMessage> Queue
        {
            get;
        }

        public IQueueMessage<ShredderBigFileBlockCopyMessage> QueueMessage
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

        public ShredderBigFileBlockCopyJobContext(
            IQueue<ShredderBigFileBlockCopyMessage> shredderBigFileBlockCopyQueue,
            IQueueMessage<ShredderBigFileBlockCopyMessage> shredderBigFileBlockCopyMessage,
            ICloudBlobClient blobClient)
        {
            GuardHelper.ArgumentNotNull(shredderBigFileBlockCopyQueue);
            GuardHelper.ArgumentNotNull(shredderBigFileBlockCopyMessage);
            GuardHelper.ArgumentNotNull(blobClient);

            this.Queue = shredderBigFileBlockCopyQueue;
            this.QueueMessage = shredderBigFileBlockCopyMessage;
            this.BlobClient = blobClient;
        }

        public virtual ICloudBlockBlob GetPartnerCloudBlockBlob()
        {
            return new AzureStorageCloudBlockBlob(new Uri(Message.PartnerBlobUri));
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
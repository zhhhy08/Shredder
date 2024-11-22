namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Web;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.TypeSpace.Contracts.ResourceProviderManifest;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ShredderBigFileBlockCopyMessage : ITrackedMessage
    {
        #region Properties

        #region TrackedMessage

        [JsonProperty]
        public Guid? CorrelationId
        {
            get;
            set;
        }

        #endregion

        [JsonProperty(Required = Required.Always)]
        public string PartnerBlobUri
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public DateTimeOffset NotificationSnapshotTime
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public IList<BlockCopyInfo> BlockCopyInfos
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public string ResourceType
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public string ContainerName
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public string SnapshotBlobPath
        {
            get;
            private set;
        }

        [JsonProperty]
        [DefaultValue(1)]
        public int Version
        {
            get;
            private set;
        }

        #endregion

        #region Constructors

        [JsonConstructor]
        protected ShredderBigFileBlockCopyMessage()
        {
        }

        public ShredderBigFileBlockCopyMessage(string partnerBlobUri,
            DateTimeOffset notificationSnapshotTime, IList<BlockCopyInfo> blockCopyInfos, Guid correlationId,
            string resourceType, string containerName, string snapshotBlobPath)
        {
            GuardHelper.ArgumentNotNull(correlationId);
            GuardHelper.ArgumentNotNullOrEmpty(partnerBlobUri);
            GuardHelper.ArgumentNotNullOrEmpty(blockCopyInfos);
            GuardHelper.ArgumentNotNullOrEmpty(resourceType);
            GuardHelper.ArgumentNotNullOrEmpty(containerName);
            GuardHelper.ArgumentNotNullOrEmpty(snapshotBlobPath);

            this.PartnerBlobUri = partnerBlobUri;
            this.NotificationSnapshotTime = notificationSnapshotTime;
            this.BlockCopyInfos = blockCopyInfos;
            this.CorrelationId = correlationId;
            this.ResourceType = resourceType;
            this.ContainerName = containerName;
            this.SnapshotBlobPath = snapshotBlobPath;
        }

        #endregion

        public static ShredderBigFileBlockCopyMessage Parse(string serializedValue)
        {
            GuardHelper.ArgumentNotNullOrEmpty(serializedValue);

            return JsonTypeFormatter.Formatter.ReadFromText<ShredderBigFileBlockCopyMessage>(serializedValue);
        }
    }
}

namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Web;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.TypeSpace.Contracts.ResourceProviderManifest;
    using Newtonsoft.Json;
    using System;
    using System.ComponentModel;

    public class ShredderProcessingMessage : ITrackedMessage
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
        public string SnapshotContainerName
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public string SnapshotBlobFolder
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

        [JsonProperty(Required = Required.Always)]
        public string NotificationResourceType
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
        public int SegmentIndex
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public int TotalSegments
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

        [JsonProperty]
        [DefaultValue("/")]
        public string ScopeId
        {
            get;
            private set;
        }

        [JsonProperty]
        [DefaultValue(ResourceProviderRoutingType.IsArgEnabled)]
        public ResourceProviderRoutingType RoutingType
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public int KustoPartitions
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public ArgNotificationMessage NotificationMessage
        {
            get;
            private set;
        }

        #endregion

        #region Constructors

        [JsonConstructor]
        protected ShredderProcessingMessage()
        {
        }

        public ShredderProcessingMessage(
            ArgNotificationMessage notificationMessage,
            string snapshotContainerName,
            string snapshotBlobFolder,
            string snapshotBlobPath,
            string patnerBlobUri,
            DateTimeOffset notificationSnapshotTime,
            int segmentIndex,
            int totalSegments,
            int kustoPartitions,
            string notificationResourceType,
            string scopeId,
            ResourceProviderRoutingType routingType,
            Guid correlationId)
        {
            GuardHelper.ArgumentNotNull(notificationMessage);
            GuardHelper.ArgumentNotNullOrEmpty(snapshotContainerName);
            GuardHelper.ArgumentNotNullOrEmpty(snapshotBlobFolder);
            GuardHelper.ArgumentNotNullOrEmpty(snapshotBlobPath);
            GuardHelper.ArgumentNotNullOrEmpty(patnerBlobUri);
            GuardHelper.IsArgumentNonNegative(segmentIndex);
            GuardHelper.IsArgumentPositive(totalSegments);
            GuardHelper.IsArgumentPositive(kustoPartitions);
            GuardHelper.ArgumentNotNullOrEmpty(scopeId);
            GuardHelper.ArgumentNotNullOrEmpty(notificationResourceType);
            GuardHelper.ArgumentNotNull(correlationId);

            this.NotificationMessage = notificationMessage;
            this.SnapshotContainerName = snapshotContainerName;
            this.SnapshotBlobFolder = snapshotBlobFolder;
            this.SnapshotBlobPath = snapshotBlobPath;
            this.PartnerBlobUri = patnerBlobUri;
            this.NotificationSnapshotTime = notificationSnapshotTime;
            this.SegmentIndex = segmentIndex;
            this.TotalSegments = totalSegments;
            this.KustoPartitions = kustoPartitions;
            this.ScopeId = scopeId;
            this.RoutingType = routingType;
            this.NotificationResourceType = notificationResourceType;
            this.CorrelationId = correlationId;
        }

        #endregion

        public static ShredderProcessingMessage Parse(string serializedValue)
        {
            GuardHelper.ArgumentNotNullOrEmpty(serializedValue);

            return JsonTypeFormatter.Formatter.ReadFromText<ShredderProcessingMessage>(serializedValue);
        }
    }
}

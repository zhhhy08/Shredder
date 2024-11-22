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

    public class ShredderBigFileAwaitCopyMessage : ITrackedMessage
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
        public ArgNotificationMessage NotificationMessage
        {
            get;
            private set;
        }

        [JsonProperty(Required = Required.Always)]
        public int PartnerBlockCount
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
        protected ShredderBigFileAwaitCopyMessage()
        {
        }

        public ShredderBigFileAwaitCopyMessage(
            ArgNotificationMessage notificationMessage,
            int partnerBlockCount,
            Guid correlationId)
        {
            GuardHelper.ArgumentNotNull(correlationId);
            GuardHelper.IsArgumentPositive(partnerBlockCount);

            this.NotificationMessage = notificationMessage;
            this.PartnerBlockCount = partnerBlockCount;
            this.CorrelationId = correlationId;
        }

        #endregion

        public static ShredderBigFileAwaitCopyMessage Parse(string serializedValue)
        {
            GuardHelper.ArgumentNotNullOrEmpty(serializedValue);

            return JsonTypeFormatter.Formatter.ReadFromText<ShredderBigFileAwaitCopyMessage>(serializedValue);
        }
    }
}

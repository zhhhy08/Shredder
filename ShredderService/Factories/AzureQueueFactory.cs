namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Factories
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.AzureStorage;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.AzureQueues;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.MessageSerializer;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceBusManagement.ServiceBus;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Config.Sources.KeyVault.Utils;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Configuration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Web;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Utils;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.WritePath.Common.Utils;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    [SuppressMessage(
        "Microsoft.Design",
        "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "Queues lifetime exceed that of settings object")]
    [ExcludeFromCodeCoverage]
    internal class AzureQueueFactory
    {
        #region Fields

        private readonly JsonFormatterQueueMessageSerializer<ArgRetryNotificationMessage>
            _argRetryNotificationMessageSerializer =
                new JsonFormatterQueueMessageSerializer<ArgRetryNotificationMessage>(
                    JsonTypeFormatter.MinifiedSerializerSettings);

        private readonly JsonFormatterQueueMessageSerializer<ShredderBigFileBlockCopyMessage> _shredderBigFileBlockCopyMessageSerializer =
            new JsonFormatterQueueMessageSerializer<ShredderBigFileBlockCopyMessage>(
                JsonTypeFormatter.MinifiedSerializerSettings);

        private readonly JsonFormatterQueueMessageSerializer<ShredderBigFileAwaitCopyMessage> _shredderBigFileAwaitCopyMessageSerializer =
            new JsonFormatterQueueMessageSerializer<ShredderBigFileAwaitCopyMessage>(
                JsonTypeFormatter.MinifiedSerializerSettings);

        private readonly JsonFormatterQueueMessageSerializer<ShredderProcessingMessage> _shredderProcessingMessageSerializer =
            new JsonFormatterQueueMessageSerializer<ShredderProcessingMessage>(
                JsonTypeFormatter.MinifiedSerializerSettings);

        private readonly JsonFormatterQueueMessageSerializer<ArgNotificationMessage> _argNotificationMessageSerializer =
            new JsonFormatterQueueMessageSerializer<ArgNotificationMessage>(
                JsonTypeFormatter.MinifiedSerializerSettings);

        private ServiceBusSettings _serviceBusSettings;

        private const QueueAccess InitialQueueAccess = QueueAccess.AllowAll;

        private static readonly string ArtSnapshotNotificationRetryQueue =
            ServiceConfiguration.Current.Get(
            ServiceConfigConstants.ResourceNotificationsSection,
            WritePath.Common.Constants.ConfigurationConstants.SnapshotNotificationRetryQueue);

        #endregion

        #region Properties

        public IQueue<ArgNotificationMessage> ShredderBigFileQueue { get; private set; }

        public IQueue<ArgNotificationMessage> ShredderBigFileFailureQueue { get; private set; }

        public IQueue<ShredderBigFileBlockCopyMessage> ShredderBigFileBlockCopyQueue { get; private set; }

        public IQueue<ShredderBigFileBlockCopyMessage> ShredderBigFileBlockCopyFailureQueue { get; private set; }

        public IQueue<ShredderBigFileAwaitCopyMessage> ShredderBigFileAwaitCopyQueue { get; private set; }

        public IQueue<ShredderBigFileAwaitCopyMessage> ShredderBigFileAwaitCopyFailureQueue { get; private set; }

        public IQueue<ShredderProcessingMessage> ShredderProcessingQueue { get; private set; }

        public IQueue<ShredderProcessingMessage> ShredderProcessingFailureQueue { get; private set; }

        public IQueue<ArgRetryNotificationMessage> SnapshotNotificationRetryQueue { get; private set; }

        #endregion

        #region Constructors

        private AzureQueueFactory()
        {
            var adminStorageAccount = ChaosAzureStorageAccountFactory.Parse(
                ServiceConfiguration.Current,
                ServiceConfigConstants.CommonSection, ServiceConfigConstants.AdminStorageAccountName,
                nameof(AzureQueueFactory));

            var shredderStorageAccounts = ShredderUtils.GetShredderStorageAccounts(ServiceConfiguration.Current);

            this._serviceBusSettings = this.GetServiceBusSettings();

            this.ShredderBigFileQueue = new AzureMultiQueue<ArgNotificationMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileQueue),
                    this._argNotificationMessageSerializer,
                    null));

            this.ShredderBigFileFailureQueue = new AzureMultiQueue<ArgNotificationMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileFailureQueue),
                    this._argNotificationMessageSerializer,
                    null));

            this.ShredderBigFileBlockCopyQueue = new AzureMultiQueue<ShredderBigFileBlockCopyMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileBlockCopyQueue),
                    this._shredderBigFileBlockCopyMessageSerializer,
                    null));

            this.ShredderBigFileBlockCopyFailureQueue = new AzureMultiQueue<ShredderBigFileBlockCopyMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileBlockCopyFailureQueue),
                    this._shredderBigFileBlockCopyMessageSerializer,
                    null));

            this.ShredderBigFileAwaitCopyQueue = new AzureMultiQueue<ShredderBigFileAwaitCopyMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileAwaitCopyQueue),
                    this._shredderBigFileAwaitCopyMessageSerializer,
                    null));

            this.ShredderBigFileAwaitCopyFailureQueue = new AzureMultiQueue<ShredderBigFileAwaitCopyMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileAwaitCopyFailureQueue),
                    this._shredderBigFileAwaitCopyMessageSerializer,
                    null));

            this.ShredderProcessingQueue = new AzureMultiQueue<ShredderProcessingMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderProcessingQueue),
                    this._shredderProcessingMessageSerializer,
                    null));

            this.ShredderProcessingFailureQueue = new AzureMultiQueue<ShredderProcessingMessage>(QueueUtilities.GetQueues(
                    ServiceConfiguration.Current,
                    shredderStorageAccounts,
                    null,
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderProcessingFailureQueue),
                    this._shredderProcessingMessageSerializer,
                    null));

            var writeQueuesStorageAccounts = ChaosAzureStorageAccountFactory.ParseList(ServiceConfiguration.Current,
                ServiceConfigConstants.CommonSection, ServiceConfigConstants.WriteQueuesStorageAccountNames,
                ';', nameof(AzureQueueFactory));

            if (ScaleUnitUtil.IsScaleUnitArg())
            {
                writeQueuesStorageAccounts.Add(adminStorageAccount);
            }

            var queueCountPerQueueTypeInServiceBus = ServiceConfiguration.Current.Get<int>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.WriteQueueCountPerQueueTypeInServiceBus);

            var writeServiceBusNames = ConfigurationUtils.ParseFromSemicolonSeparatedList<string>(
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ResourceNotificationsSection,
                        ServiceConfigConstants.WriteServiceBusNameList));

            this.AssignWriteRetryQueues(
                ServiceConfiguration.Current,
                writeQueuesStorageAccounts,
                writeServiceBusNames,
                queueCountPerQueueTypeInServiceBus);
        }

        private void AssignWriteRetryQueues(
            IConfiguration configuration,
            IList<ICloudStorageAccount> writeQueuesStorageAccounts,
            IList<string> serviceBusNames,
            int queueCountPerQueueTypeInServiceBus)
        {
            var snapshotNotificationRetryQueues = QueueUtilities.GetQueues<ArgRetryNotificationMessage>(
                configuration,
                writeQueuesStorageAccounts,
                serviceBusNames,
                ArtSnapshotNotificationRetryQueue,
                this._argRetryNotificationMessageSerializer,
                this._serviceBusSettings,
                queueCountPerQueueTypeInServiceBus,
                InitialQueueAccess);

            this.SnapshotNotificationRetryQueue =
                new AzureParallelMultiQueue<ArgRetryNotificationMessage>(snapshotNotificationRetryQueues);
        }

        private ServiceBusSettings GetServiceBusSettings()
        {
            var getTimeout = TimeSpan.Parse(ServiceConfiguration.Current.Get(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusGetTimeout));
            var generalTimeout = TimeSpan.Parse(ServiceConfiguration.Current.Get(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusGeneralTimeout));
            var sdkPrefetchCount = ServiceConfiguration.Current.Get<int>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusSdkPrefetchCount);
            var lenCachingInSec = ServiceConfiguration.Current.Get<int>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusLenCachingInSeconds);
            var shallowSdkPrefetchMode = ServiceConfiguration.Current.Get<bool>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusShallowSdkPrefetchMode);
            var returnedMessagesCounterLookBackInMinutes = ServiceConfiguration.Current.Get<int>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusReturnedMessagesCounterLookBackInMinutes);
            var returnedMessagesCounterPercentage = ServiceConfiguration.Current.Get<int>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusReturnedMessagesCounterPercentage);
            var clientPrefetchEnabled = ServiceConfiguration.Current.Get<bool>(
                ServiceConfigConstants.ResourceNotificationsSection,
                ServiceConfigConstants.ServiceBusClientPrefetchEnabled);

            return new ServiceBusSettings(getTimeout, generalTimeout, sdkPrefetchCount, lenCachingInSec, shallowSdkPrefetchMode,
                returnedMessagesCounterLookBackInMinutes: returnedMessagesCounterLookBackInMinutes, returnedMessagesCounterPercentage: returnedMessagesCounterPercentage,
                clientPrefetchEnabled: clientPrefetchEnabled);
        }

        #endregion

        #region Singleton Impl

        public static AzureQueueFactory Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (SyncRoot)
                    {
                        if (_instance == null)
                        {
                            _instance = new AzureQueueFactory();
                        }
                    }
                }

                return _instance;
            }
        }

        private static readonly object SyncRoot = new object();

        private static volatile AzureQueueFactory _instance;

        #endregion

        public async Task CreateQueuesIfNotExistAsync(CancellationToken cancellationToken)
        {
            await this.ShredderBigFileQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
            await this.ShredderBigFileBlockCopyQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
            await this.ShredderBigFileAwaitCopyQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
            await this.ShredderProcessingQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();

            await this.ShredderBigFileFailureQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
            await this.ShredderBigFileBlockCopyFailureQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();           
            await this.ShredderBigFileAwaitCopyFailureQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
            await this.ShredderProcessingFailureQueue.CreateIfNotExistsAsync(cancellationToken).IgnoreContext();
        }
    }
}
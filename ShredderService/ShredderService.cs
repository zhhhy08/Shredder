namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService
{
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.QueueKpiCollector;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Config;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Config.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceMonitoring;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Configuration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedContracts.Contexts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedServiceImplementations.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedServiceImplementations.Startup;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Factories;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobManagers;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.QueueHandlers;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.TimerTasks;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Utils;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.WritePath.Common.Utils;
    using PartitionedQueue.QueueConsumer;
    using ServiceConfiguration;
    using ServiceConfiguration.Constants;
    using SharedImplementations.Factories;
    using SharedServiceImplementations.ServiceFabric;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Fabric;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    /// <inheritdoc />
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    [ExcludeFromCodeCoverage]
    internal sealed class ShredderService : ArgDefaultStatelessService
    {
        #region Tracing

        private static readonly ActivityMonitorFactory ShredderServiceRunAsyncFactory =
            new ActivityMonitorFactory("ShredderService.RunAsync");
        private static readonly ActivityMonitorFactory ShredderServiceUpdateDeploymentStateAsyncFactory =
            new ActivityMonitorFactory("ShredderService.UpdateDeploymentStateAsync");

        #endregion

        #region ArgDefaultStatelessService Implementation

        protected override ActivityMonitorFactory RunAsyncMonitorFactory => ShredderServiceRunAsyncFactory;

        protected override ActivityMonitorFactory UpdateDeploymentStateAsyncFactory => ShredderServiceUpdateDeploymentStateAsyncFactory;

        protected override PlatformServiceContext? Service => PlatformServiceContext.ShredderService;

        #endregion

        #region Fields

        #region Job managers

        private ShredderBigFileStartCopyJobManager _shredderBigFileStartCopyJobManager;

        private ShredderBigFileBlockCopyJobManager _shredderBigFileBlockCopyJobManager;

        private ShredderBigFileAwaitCopyJobManager _shredderBigFileAwaitCopyJobManager;

        private ShredderProcessingJobManager _shredderProcessingJobManager;

        #endregion

        #region

        private IList<GroomShredderSnapshotBlobTimerTask> _groomShredderSnapshotBlobTimerTasks;

        #endregion

        #region Properties

        private QueueMessagesConsumer<ArgNotificationMessage> _shredderBigFilePoisonQueueHandlerConsumer;

        private QueueSizeCollector<ArgNotificationMessage> _shredderBigFilePoisonQueueSizeCollector;

        private QueueMessagesConsumer<ShredderBigFileBlockCopyMessage> _shredderBigFileBlockCopyPoisonQueueHandlerConsumer;

        private QueueSizeCollector<ShredderBigFileBlockCopyMessage> _shredderBigFileBlockCopyPoisonQueueSizeCollector;

        private QueueMessagesConsumer<ShredderBigFileAwaitCopyMessage> _shredderBigFileAwaitCopyPoisonQueueHandlerConsumer;

        private QueueSizeCollector<ShredderBigFileAwaitCopyMessage> _shredderBigFileAwaitCopyPoisonQueueSizeCollector;

        private QueueMessagesConsumer<ShredderProcessingMessage> _shredderProcessingPoisonQueueConsumer;

        private QueueSizeCollector<ShredderProcessingMessage> _shredderProcessingPoisonQueueSizeCollector;

        public static CosmosDbConfigurationKeys CosmosDbConfigurationKeys => ConfigurationSetting.CosmosDbConfigurationKeys;

        private static readonly ConfigurationSetting ConfigurationSetting = ConfigurationSetting.ServiceFabricWithScaleUnitHotConfigWithMsi;

        #endregion

        #endregion

        #region Constructors 

        public ShredderService(StatelessServiceContext context)
            : base(context, ConfigurationSetting, ScaleUnitUtil.GetScaleUnitDimensions)
        {
            ThreadPoolManager.ConfigureThreadPool();

            ConfigureOptOutActivities();
        }

        #endregion

        #region StatelessService

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP)
        /// for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<
            ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new[]
            {
                new ServiceInstanceListener(
                    serviceContext => new KestrelCommunicationListener(
                        serviceContext, ServiceConfigConstants.HttpsServiceEndpoint,
                        (url, listener) =>
                            new WebHostBuilder()
                                .UseKestrel(options =>
                                {
                                    int port = serviceContext.CodePackageActivationContext.GetEndpoint(ServiceConfigConstants.HttpsServiceEndpoint).Port;
                                    options.Listen(IPAddress.IPv6Any, port);
                                    // ARM can send up to 5 security group headers of up to 64 KB, allow an extra for other ARM headers.
                                    options.Limits.MaxRequestHeadersTotalSize = 6*64*1024;
                                })
                                .UseSockets(options => options.NoDelay = true)
                                .UseStartup<Startup>()
                                .UseUrls(url)
                                .Build()), name: ServiceConfigConstants.HttpsServiceEndpoint)
            };
        }

        protected override async Task RunAsyncHelper(IActivity parentActivity, CancellationToken cancellationToken)
        {
            if(ScaleUnitUtil.IsEnvironmentOrScaleUnitEnabledForShredder())
            {
                await this.CreateQueuesIfNotExistAsync(cancellationToken).IgnoreContext();
                this.InitializeTimerTasks();
                this.InitializeJobManagers();
                this.InitializeQueueConsumers();
            }
        }

        #endregion

        #region Private/Helper methods

        private static void ConfigureOptOutActivities()
        {
            var optedOutActivities = ServiceConfiguration.Current.Get<List<string>>(
                ServiceConfigConstants.MonitoringSection,
                ConfigurationConstants.ShredderOptedOutActivities);

            TracingConfiguration.UpdateOptOutActivitiesForLogs(optedOutActivities);

            ServiceConfiguration.Current.Requirement
                .Add<List<string>>(ServiceConfigConstants.MonitoringSection, ConfigurationConstants.ShredderOptedOutActivities)
                .RegisterCallback(TracingConfiguration.UpdateOptOutActivitiesForLogs);

            ServiceConfiguration.Current.Requirement.Add<bool>(
                   ServiceConfigConstants.MonitoringSection,
                   ConfigurationConstants.IsActivityStartedDisabled)
               .RegisterCallbackAndInitialize(TracingConfiguration.IsActivityStartedDisabledConfigCallback);

            var unskippableStartedEventsForLogs = ServiceConfiguration.Current.Get<List<string>>(
                ServiceConfigConstants.MonitoringSection,
                ConfigurationConstants.UnskippableStartedEventsForLogs);

            TracingConfiguration.UpdateUnskippableStartedEventsForLogs(unskippableStartedEventsForLogs);

            ServiceConfiguration.Current.Requirement
               .Add<List<string>>(ServiceConfigConstants.MonitoringSection, ConfigurationConstants.UnskippableStartedEventsForLogs)
               .RegisterCallback(TracingConfiguration.UpdateUnskippableStartedEventsForLogs);
        }


        protected override Task UpdateDeploymentStateHelperAsync(StampState stampState, bool runAsyncCancelled, IActivity parentActivity, CancellationToken cancellationToken)
        {
            switch (stampState)
            {
                case StampState.On:
                case StampState.NonDeploymentValidation:
                    this.StartTimerTasks();
                    break;
                default:
                    this.StopTimerTasks();
                    break;
            }

            return Task.CompletedTask;
        }

        private async Task CreateQueuesIfNotExistAsync(CancellationToken cancellationToken)
        {
            await AzureQueueFactory.Instance.CreateQueuesIfNotExistAsync(cancellationToken).IgnoreContext();
        }

        private void InitializeQueueConsumers()
        {
            var queueConsumerSettings = new QueueMessagesConsumerSettings
            {
                MessageCapacity = 8,

                // We do not want poison message handling 
                // so set PoisonDequeueCount to Max value
                PoisonMessageDequeueCount = int.MaxValue,
                GetMessageVisibilityTimeout = TimeSpan.FromSeconds(60)
            };

            var replayMessageClientFactory = ReplayMessageClientFactory.GetInstance(CosmosDbConfigurationKeys);
            var shredderBigFilePoisonQueueHandler = new ShredderBigFilePoisonQueueHandler(replayMessageClientFactory.Client);
            var shredderBigFileBlockCopyPoisonQueueHandler = new ShredderBigFileBlockCopyPoisonQueueHandler(replayMessageClientFactory.Client);
            var shredderBigFileAwaitCopyPoisonQueueHandler = new ShredderBigFileAwaitCopyPoisonQueueHandler(replayMessageClientFactory.Client);
            var shredderProcessingPoisonQueueHandler = new ShredderProcessingPoisonQueueHandler(replayMessageClientFactory.Client);

            this._shredderBigFilePoisonQueueHandlerConsumer = new QueueMessagesConsumer<ArgNotificationMessage>(
                    AzureQueueFactory.Instance.ShredderBigFileFailureQueue,
                    shredderBigFilePoisonQueueHandler,
                    queueConsumerSettings);
            this._shredderBigFilePoisonQueueSizeCollector = new QueueSizeCollector<ArgNotificationMessage>(
                    AzureQueueFactory.Instance.ShredderBigFileFailureQueue,
                    shredderBigFilePoisonQueueHandler);

            this._shredderBigFileBlockCopyPoisonQueueHandlerConsumer = new QueueMessagesConsumer<ShredderBigFileBlockCopyMessage>(
                    AzureQueueFactory.Instance.ShredderBigFileBlockCopyFailureQueue,
                    shredderBigFileBlockCopyPoisonQueueHandler,
                    queueConsumerSettings);
            this._shredderBigFileBlockCopyPoisonQueueSizeCollector = new QueueSizeCollector<ShredderBigFileBlockCopyMessage>(
                    AzureQueueFactory.Instance.ShredderBigFileBlockCopyFailureQueue,
                    shredderBigFileBlockCopyPoisonQueueHandler);

            this._shredderBigFileAwaitCopyPoisonQueueHandlerConsumer = new QueueMessagesConsumer<ShredderBigFileAwaitCopyMessage>(
                    AzureQueueFactory.Instance.ShredderBigFileAwaitCopyFailureQueue,
                    shredderBigFileAwaitCopyPoisonQueueHandler,
                    queueConsumerSettings);

            this._shredderBigFileAwaitCopyPoisonQueueSizeCollector = new QueueSizeCollector<ShredderBigFileAwaitCopyMessage>(
                    AzureQueueFactory.Instance.ShredderBigFileAwaitCopyFailureQueue,
                    shredderBigFileAwaitCopyPoisonQueueHandler);

            this._shredderProcessingPoisonQueueConsumer = new QueueMessagesConsumer<ShredderProcessingMessage>(
                   AzureQueueFactory.Instance.ShredderProcessingFailureQueue,
                   shredderProcessingPoisonQueueHandler,
                   queueConsumerSettings);
            this._shredderProcessingPoisonQueueSizeCollector = new QueueSizeCollector<ShredderProcessingMessage>(
                    AzureQueueFactory.Instance.ShredderProcessingFailureQueue,
                    shredderProcessingPoisonQueueHandler);
        }

        private void StartQueueSizeCollectors()
        {
            this._shredderBigFilePoisonQueueSizeCollector?.StartAsync();
            this._shredderBigFileBlockCopyPoisonQueueSizeCollector?.StartAsync();
            this._shredderBigFileAwaitCopyPoisonQueueSizeCollector?.StartAsync();
            this._shredderProcessingPoisonQueueSizeCollector?.StartAsync();
        }

        private void StopQueueSizeCollectors()
        {
            this._shredderBigFilePoisonQueueSizeCollector?.Stop();
            this._shredderBigFileBlockCopyPoisonQueueSizeCollector?.StartAsync();
            this._shredderBigFileAwaitCopyPoisonQueueSizeCollector?.StartAsync();
            this._shredderProcessingPoisonQueueSizeCollector?.Stop();
        }

        private void StartQueueConsumers()
        {
            this._shredderBigFilePoisonQueueHandlerConsumer?.StartAsync();
            this._shredderBigFileBlockCopyPoisonQueueHandlerConsumer?.StartAsync();
            this._shredderBigFileAwaitCopyPoisonQueueHandlerConsumer?.StartAsync();
            this._shredderProcessingPoisonQueueConsumer?.StartAsync();
        }

        private void StopQueueConsumers()
        {
            this._shredderBigFilePoisonQueueHandlerConsumer?.Stop();
            this._shredderBigFileBlockCopyPoisonQueueHandlerConsumer?.Stop();
            this._shredderBigFileAwaitCopyPoisonQueueHandlerConsumer?.Stop();
            this._shredderProcessingPoisonQueueConsumer?.Stop();
        }

        private void InitializeJobManagers()
        {
            this._shredderBigFileStartCopyJobManager =
                    new ShredderBigFileStartCopyJobManager(
                        ServiceConfiguration.Current.Get<int>(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileJobManagerThreadPoolThreadLimitEntry),
                        AzureQueueFactory.Instance.ShredderBigFileQueue,
                        AzureQueueFactory.Instance.ShredderBigFileFailureQueue);
            this._shredderBigFileBlockCopyJobManager =
                    new ShredderBigFileBlockCopyJobManager(
                        ServiceConfiguration.Current.Get<int>(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileBlockCopyJobManagerThreadPoolThreadLimitEntry),
                        AzureQueueFactory.Instance.ShredderBigFileBlockCopyQueue,
                        AzureQueueFactory.Instance.ShredderBigFileBlockCopyFailureQueue);
            this._shredderBigFileAwaitCopyJobManager =
                    new ShredderBigFileAwaitCopyJobManager(
                        ServiceConfiguration.Current.Get<int>(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileAwaitCopyJobManagerThreadPoolThreadLimitEntry),
                        AzureQueueFactory.Instance.ShredderBigFileAwaitCopyQueue,
                        AzureQueueFactory.Instance.ShredderBigFileAwaitCopyFailureQueue);
            this._shredderProcessingJobManager =
                new ShredderProcessingJobManager(
                    ServiceConfiguration.Current.Get<int>(
                    ServiceConfigConstants.ShredderSection,
                    ConfigurationConstants.ShredderProcessingJobManagerThreadPoolThreadLimitEntry),
                    AzureQueueFactory.Instance.ShredderProcessingQueue,
                    AzureQueueFactory.Instance.ShredderProcessingFailureQueue);
        }
        private void InitializeTimerTasks()
        {
            this._groomShredderSnapshotBlobTimerTasks = new List<GroomShredderSnapshotBlobTimerTask>();
            var shredderStorageAccounts = ShredderUtils.GetShredderStorageAccounts(ServiceConfiguration.Current);
            var shredderBlobClients = shredderStorageAccounts.Select(
                shredderStorageAccount => shredderStorageAccount.CreateCloudBlobClient())
                .ToList();

            var snapshotResourceTypeToSnapshotNames = ConfigurationUtils.ParseDictionaryFromString(
                ServiceConfiguration.Current.Get(
                    ServiceConfigConstants.ShredderSection,
                    ConfigurationConstants.SnapshotResourceTypeToSnapshotName))
                .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);

            foreach (var snapshotResourceTypeToSnapshotName in snapshotResourceTypeToSnapshotNames)
            {
                this._groomShredderSnapshotBlobTimerTasks.Add(new GroomShredderSnapshotBlobTimerTask(
                   JobContextFactory.Instance.GetBlobClientForShredderSnapshotResourceFromShredderStorageAccount(
                       shredderBlobClients,
                       snapshotResourceTypeToSnapshotName.Key),
                   snapshotResourceTypeToSnapshotName.Value));
            }
        }

        private void StartJobManagers()
        {
            this._shredderBigFileStartCopyJobManager?.StartAsync();
            this._shredderBigFileBlockCopyJobManager?.StartAsync();
            this._shredderBigFileAwaitCopyJobManager?.StartAsync();
            this._shredderProcessingJobManager?.StartAsync();
        }

        private void StopJobManagers()
        {
            this._shredderBigFileStartCopyJobManager?.Stop();
            this._shredderBigFileBlockCopyJobManager?.Stop();
            this._shredderBigFileAwaitCopyJobManager?.Stop();
            this._shredderProcessingJobManager?.Stop();
        }

        private void StartGroomingTimerTasks()
        {
            foreach (var groomShredderSnapshotBlobTimerTask in this._groomShredderSnapshotBlobTimerTasks)
            {
                groomShredderSnapshotBlobTimerTask?.StartAsync();
            }
        }

        private void StopAndDisposeGroomingTimerTasks()
        {
            foreach (var groomShredderSnapshotBlobTimerTask in this._groomShredderSnapshotBlobTimerTasks)
            {
                groomShredderSnapshotBlobTimerTask?.Stop();
            }
        }

        private void StartTimerTasks()
        {
            if (ScaleUnitUtil.IsEnvironmentOrScaleUnitEnabledForShredder())
            {
                this.StartJobManagers();
                this.StartQueueSizeCollectors();
                this.StartQueueConsumers();
                this.StartGroomingTimerTasks();
            }
        }

        private void StopTimerTasks()
        {
            if (ScaleUnitUtil.IsEnvironmentOrScaleUnitEnabledForShredder())
            {
                this.StopJobManagers();
                this.StopQueueSizeCollectors();
                this.StopQueueConsumers();
                this.StopAndDisposeGroomingTimerTasks();
            }
        }

        #endregion
    }
}
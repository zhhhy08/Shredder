namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobManagers
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.AzureStorage;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts.Blob;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.JobManager;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Jobs.Base;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Tracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Notifications.Shared.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Config.Sources.KeyVault.Utils;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Configuration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy.RetryStrategy;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedContracts.Common;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Configs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Factories;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Jobs.ShredderBigFileAwaitCopyJobs;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Utils;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;

    /// <seealso cref="JobManagement.JobManager.JobManager{ShredderBigFileAwaitCopyJobContext, ShredderBigFileAwaitCopyMessage}" />
    internal class ShredderBigFileAwaitCopyJobManager : JobManager<ShredderBigFileAwaitCopyJobContext, ShredderBigFileAwaitCopyMessage>
    {
        #region Tracing

        private static readonly MetricActivityMonitorFactory ShredderBigFileAwaitCopyJobManagerDoWorkAsync =
            new MetricActivityMonitorFactory("ShredderBigFileAwaitCopyJobManager.DoWorkAsync",
                IfxTracingJobManagement.JobQueueLengthMetric, IfxTracingJobManagement.JobQueueLengthProperty,
                IfxTracingJobManagement.PoisonQueueLengthMetric, IfxTracingJobManagement.PoisonQueueLengthProperty);

        private static readonly MetricActivityMonitorFactory ShredderBigFileAwaitCopyJobManagerAssignAndStartJobsAsyncFactory =
            new MetricActivityMonitorFactory("ShredderBigFileAwaitCopyJobManager.AssignAndStartJobsAsync",
                IfxTracingJobManagement.AssignedJobsCountMetric, IfxTracingJobManagement.AssignedJobsCountProperty,
                IfxTracingJobManagement.FreeJobPercentageMetric, IfxTracingJobManagement.FreeJobPercentageProperty,
                IfxTracingJobManagement.MaxVisibilityDelayMetric, IfxTracingJobManagement.MaxVisibilityDelayProperty,
                IfxTracingJobManagement.MinVisibilityDelayMetric, IfxTracingJobManagement.MinVisibilityDelayProperty,
                IfxTracingJobManagement.AverageVisibilityDelayMetric,
                IfxTracingJobManagement.AverageVisibilityDelayProperty);

        #endregion

        #region Members

        private static readonly IRetryStrategy RetryStrategy;

        #endregion

        #region Properties

        protected override MetricActivityMonitorFactory DoWorkAsyncFactory =>
            ShredderBigFileAwaitCopyJobManagerDoWorkAsync;

        protected override MetricActivityMonitorFactory AssignAndStartJobsAsyncFactory =>
            ShredderBigFileAwaitCopyJobManagerAssignAndStartJobsAsyncFactory;

        protected override string Scenario => ScenarioConstants.Shredder;

        public IList<ICloudStorageAccount> ShredderStorageAccounts
        {
            get;
            private set;
        }

        public IList<ICloudBlobClient> ShredderBlobClients
        {
            get;
            private set;
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes the <see cref="ShredderBigFileAwaitCopyJobManager"/> class.
        /// </summary>
        [SuppressMessage(
            "Microsoft.Performance",
            "CA1810:InitializeReferenceTypeStaticFieldsInline",
            Justification = "Need initialization of non explicit static fields")]
        static ShredderBigFileAwaitCopyJobManager()
        {
            RetryStrategy = new ExponentialBackoff(
                ServiceConfiguration.Current.Get<int>(
                    ServiceConfigConstants.ShredderSection,
                    ConfigurationConstants.ShredderBigFileAwaitCopyJobManagerMaxQueueMessageDequeueCountEntry),
                TimeSpan.Zero,
                TimeSpan.Parse(
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileAwaitCopyJobManagerQueueMessageMaxVisibilityTimeoutEntry)),
                TimeSpan.Parse(
                    ServiceConfiguration.Current.Get(
                        ServiceConfigConstants.ShredderSection,
                        ConfigurationConstants.ShredderBigFileAwaitCopyJobManagerDeltaQueueMessageVisibilityTimeoutEntry)));
        }

        public ShredderBigFileAwaitCopyJobManager(int threadPoolLimit,
            IQueue<ShredderBigFileAwaitCopyMessage> jobQueue,
            IQueue<ShredderBigFileAwaitCopyMessage> poisonJobQueue)
            : base(threadPoolLimit,
                jobQueue,
                poisonJobQueue,
                RetryStrategy,
                ConfigurationConstants.QueueMessageVisibilityTimeout,
                (int)ConfigurationConstants.QueueMessageVisibilityTimeout.TotalSeconds,
                (int)ConfigurationConstants.QueueMessageVisibilityCheckAhead.TotalSeconds,
                pollingPeriod: TimeSpan.Parse(ServiceConfiguration.Current.Get(
                ServiceConfigConstants.ShredderSection,
                ConfigurationConstants.ShredderBigFileAwaitCopyQueuePollingPeriod)))
        {
            this.ShredderStorageAccounts = ShredderUtils.GetShredderStorageAccounts(ServiceConfiguration.Current);
            this.ShredderBlobClients = this.ShredderStorageAccounts.Select(
                shredderStorageAccount => shredderStorageAccount.CreateCloudBlobClient())
                .ToList();
        }

        #endregion

        #region JobManager Impl

        /// <summary>
        /// Creates the job.
        /// </summary>
        protected override Job<ShredderBigFileAwaitCopyJobContext> CreateJob()
        {
            return new ShredderBigFileAwaitCopyJob();
        }

        protected override Task<ShredderBigFileAwaitCopyJobContext> CreateJobContextAsync(
                    IQueueMessage<ShredderBigFileAwaitCopyMessage> queueMessage, IActivity parentActivity)
        {
            return Task.FromResult(
                     JobContextFactory.Instance.CreateShredderBigFileAwaitCopyJobContext(
                         this.JobQueue,
                         AzureQueueFactory.Instance.ShredderProcessingQueue,
                         queueMessage,
                         this.ShredderBlobClients));
        }

        #endregion

        /// <summary>
        /// Gets the expected size of the job executor pool.
        /// </summary>
        /// <param name="parentActivity">The parent activity.</param>
        protected override int GetExpectedJobExecutorPoolSize(IActivity parentActivity)
        {
            return ServiceConfiguration.Current.Get<int>(ServiceConfigConstants.ShredderSection,
                ConfigurationConstants.ShredderBigFileAwaitCopyJobManagerThreadPoolThreadLimitEntry);
        }
    }
}

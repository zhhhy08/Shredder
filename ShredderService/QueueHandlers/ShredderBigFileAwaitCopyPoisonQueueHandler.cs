namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.QueueHandlers
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.CosmosDbDataAccessLayer.ErrorStrategy;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.JobManagement.Tracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.Contracts.Messages;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.QueueConsumer.Handlers;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.PartitionedQueue.QueueKpiCollector.Handlers;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceMonitoring.Ifx;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.RetryPolicy.Contracts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Utilities;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Web;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedContracts.CosmosDb.ReplayMessage;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.JobMessages;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public class ShredderBigFileAwaitCopyPoisonQueueHandler : IQueueMessageHandler<ShredderBigFileAwaitCopyMessage>, IQueueKpiHandler
    {
        #region Tracing

        private static readonly ActivityMonitorFactory ShredderBigFileAwaitCopyQueuePoisonHandlerProcessMessageAsync =
            new ActivityMonitorFactory("ShredderBigFileAwaitCopyPoisonQueueHandler.ProcessMessageAsync");

        private static readonly ActivityMonitorFactory ShredderBigFileAwaitCopyQueuePoisonHandlerLogQueueSizeAsync =
            new MetricActivityMonitorFactory(
                "ShredderBigFileAwaitCopyPoisonQueueHandler.LogQueueSizeAsync",
                IfxTracingJobManagement.PoisonQueueLengthMetric,
                IfxTracingJobManagement.PoisonQueueLengthProperty);

        #endregion

        #region Members

        /// <summary>
        /// The update retry policy
        /// </summary>
        private static readonly IRetryPolicy UpdateRetryPolicy =
            new RetryPolicy(new CatchUpdateErrorStrategy(), 5, TimeSpan.FromSeconds(3));

        #endregion

        #region Members

        private readonly IReplayMessageClient _replayMessageClient;

        #endregion

        #region Constructors

        public ShredderBigFileAwaitCopyPoisonQueueHandler(IReplayMessageClient replayMessageClient)
        {
            GuardHelper.ArgumentNotNull(replayMessageClient);

            this._replayMessageClient = replayMessageClient;
        }

        #endregion

        #region IQueueKpiHandler

        /// <summary>
        /// Logs the queue size asynchronous.
        /// </summary>
        /// <param name="queueSize">Size of the queue.</param>
        /// <param name="token">The token.</param>
        public Task LogQueueSizeAsync(long queueSize, CancellationToken token)
        {
            var methodMonitor = ShredderBigFileAwaitCopyQueuePoisonHandlerLogQueueSizeAsync.ToMonitor();
            methodMonitor.OnStart();

            try
            {
                methodMonitor.Activity.Properties[IfxTracingJobManagement.PoisonQueueLengthProperty] = queueSize;

                methodMonitor.OnCompleted();
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                methodMonitor.OnError(ex);
                throw;
            }
        }

        #endregion

        #region IQueueMessageHandler

        /// <summary>
        /// Processes the message asynchronous.
        /// </summary>
        /// <param name="queueMessage">The queue message.</param>
        /// <param name="token">The token.</param>
        public async Task ProcessMessageAsync(IQueueMessage<ShredderBigFileAwaitCopyMessage> queueMessage, CancellationToken token)
        {
            GuardHelper.ArgumentNotNull(queueMessage);
            GuardHelper.ArgumentNotNull(queueMessage.Message);
            var shredderBigFileAwaitCopyMessage = queueMessage.Message;
            var methodMonitor = ShredderBigFileAwaitCopyQueuePoisonHandlerProcessMessageAsync.ToMonitor();
            methodMonitor.Activity.Properties["PartnerBlobUri"] = BlobLoggingUtils.HideSigFromBlobUri(shredderBigFileAwaitCopyMessage.NotificationMessage.DataUri);
            methodMonitor.Activity.Properties["NotificationSnapshotTime"] = shredderBigFileAwaitCopyMessage.NotificationMessage.Timestamp;
            methodMonitor.OnStart();

            try
            {
                var replayMessage = new ReplayMessage(
                    JsonTypeFormatter.Formatter.WriteToText(shredderBigFileAwaitCopyMessage),
                    ReplayMessageType.ShredderBigFileAwaitCopyMessage, string.Empty);

                methodMonitor.Activity.Properties["MessageId"] = replayMessage.MessageId;
                methodMonitor.Activity.Properties["MessageType"] = replayMessage.MessageType;

                var dimensions = new[]
                {
                    "ProviderNamespace", shredderBigFileAwaitCopyMessage.NotificationMessage.ProviderNamespace,
                    "ArmEventTimestamp", shredderBigFileAwaitCopyMessage.NotificationMessage.Timestamp.ToString()
                };

                var ShredderBigFileAwaitCopyMessageFailureMetric = IfxTracingConstants.ShredderBigFileAwaitCopyMessageFailureMetric;

                IfxMetricLogger.LogMetricValue(
                    ShredderBigFileAwaitCopyMessageFailureMetric,
                    1,
                    parentActivity: methodMonitor.Activity,
                    metricDimensionNamesValues: dimensions);

                await UpdateRetryPolicy
                    .ExecuteAsync(async () => await this._replayMessageClient.CreateReplayMessageAsync(
                            replayMessage, token, methodMonitor.Activity).IgnoreContext())
                    .IgnoreContext();

                methodMonitor.OnCompleted();
            }
            catch (Exception ex)
            {
                methodMonitor.OnError(ex);
                throw;
            }
        }

        /// <summary>
        /// Processes the poison message asynchronous.
        /// </summary>
        /// <param name="queueMessage">The queue message.</param>
        /// <param name="token">The token.</param>
        public Task ProcessPoisonMessageAsync(IQueueMessage<ShredderBigFileAwaitCopyMessage> queueMessage, CancellationToken token)
        {
            // Nothing to do
            return Task.CompletedTask;
        }

        /// <summary>
        /// Processes the messages asynchronous.
        /// </summary>
        /// <param name="queueMessages">The queue messages.</param>
        /// <param name="token">The token.</param>
        /// <exception cref="NotImplementedException">Bulk processing is not supported</exception>
        public Task ProcessMessagesAsync(IList<IQueueMessage<ShredderBigFileAwaitCopyMessage>> queueMessages, CancellationToken token)
        {
            throw new NotImplementedException("Bulk processing is not supported");
        }

        #endregion
    }
}
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

    public class ShredderBigFileBlockCopyPoisonQueueHandler : IQueueMessageHandler<ShredderBigFileBlockCopyMessage>, IQueueKpiHandler
    {
        #region Tracing

        private static readonly ActivityMonitorFactory ShredderBigFileBlockCopyQueuePoisonHandlerProcessMessageAsync =
            new ActivityMonitorFactory("ShredderBigFileBlockCopyPoisonQueueHandler.ProcessMessageAsync");

        private static readonly ActivityMonitorFactory ShredderBigFileBlockCopyQueuePoisonHandlerLogQueueSizeAsync =
            new MetricActivityMonitorFactory(
                "ShredderBigFileBlockCopyPoisonQueueHandler.LogQueueSizeAsync",
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

        public ShredderBigFileBlockCopyPoisonQueueHandler(IReplayMessageClient replayMessageClient)
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
            var methodMonitor = ShredderBigFileBlockCopyQueuePoisonHandlerLogQueueSizeAsync.ToMonitor();
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
        public async Task ProcessMessageAsync(IQueueMessage<ShredderBigFileBlockCopyMessage> queueMessage, CancellationToken token)
        {
            GuardHelper.ArgumentNotNull(queueMessage);
            GuardHelper.ArgumentNotNull(queueMessage.Message);
            var shredderBigFileBlockCopyMessage = queueMessage.Message;
            var methodMonitor = ShredderBigFileBlockCopyQueuePoisonHandlerProcessMessageAsync.ToMonitor();
            methodMonitor.Activity.Properties["PartnerBlobUri"] = BlobLoggingUtils.HideSigFromBlobUri(shredderBigFileBlockCopyMessage.PartnerBlobUri);
            methodMonitor.Activity.Properties["NotificationSnapshotTime"] = shredderBigFileBlockCopyMessage.NotificationSnapshotTime;
            methodMonitor.OnStart();

            try
            {
                var replayMessage = new ReplayMessage(
                    JsonTypeFormatter.Formatter.WriteToText(shredderBigFileBlockCopyMessage),
                    ReplayMessageType.ShredderBigFileBlockCopyMessage, string.Empty);

                methodMonitor.Activity.Properties["MessageId"] = replayMessage.MessageId;
                methodMonitor.Activity.Properties["MessageType"] = replayMessage.MessageType;

                var dimensions = new[]
                {
                    "ContainerName", shredderBigFileBlockCopyMessage.ContainerName,
                    "ArmEventTimestamp", shredderBigFileBlockCopyMessage.NotificationSnapshotTime.ToString()
                };

                var ShredderBigFileBlockCopyMessageFailureMetric = IfxTracingConstants.ShredderBigFileBlockCopyMessageFailureMetric;

                IfxMetricLogger.LogMetricValue(
                    ShredderBigFileBlockCopyMessageFailureMetric,
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
        public Task ProcessPoisonMessageAsync(IQueueMessage<ShredderBigFileBlockCopyMessage> queueMessage, CancellationToken token)
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
        public Task ProcessMessagesAsync(IList<IQueueMessage<ShredderBigFileBlockCopyMessage>> queueMessages, CancellationToken token)
        {
            throw new NotImplementedException("Bulk processing is not supported");
        }

        #endregion
    }
}
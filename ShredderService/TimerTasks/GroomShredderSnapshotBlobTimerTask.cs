namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.TimerTasks
{
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts.Blob;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.TimerTasks;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Constants;
    using System;
    using System.Diagnostics.CodeAnalysis;

    public class GroomShredderSnapshotBlobTimerTask : GroomBlobContainerTimerTaskBase
    {
        protected override TimeSpan RetentionPeriod => TimeSpan.FromDays(4);

        protected override string ContainersGroomedProperty => IfxTracingConstants.ShredderSnapshotToGroomMetric;
        
        protected override ActivityMonitorFactory DoWorkAsyncFactory
        {
            get
            {
                return ShredderSnapshotBlobTimerTaskDoWorkAsync;
            }
        }

        protected override string ContainerPrefix
        {
            get
            {
                return $"{GroomShredderSnapshotBlobTimerTask.ShredderSnapshotPrefix}-{this._type}-";
            }
        }

        protected override string ContainerDateTimeSuffixFormat => GroomShredderSnapshotBlobTimerTask.ShredderSnapshotPrefixFormat;

        private readonly string _type;

        private static readonly string ShredderSnapshotPrefix = "shreddersnapshot";

        [StringSyntax(StringSyntaxAttribute.DateTimeFormat)]
        public const string ShredderSnapshotPrefixFormat = "yyyyMMddHH";

        #region Tracing

        // There is a monitor on this activity, please be careful with renaming
        private static readonly ActivityMonitorFactory ShredderSnapshotBlobTimerTaskDoWorkAsync =
            new MetricActivityMonitorFactory("ShredderSnapshotBlobTimerTaskDoWorkAsync.DoWorkAsync",
                IfxTracingConstants.ShredderSnapshotToGroomMetric, IfxTracingConstants.ShredderSnapshotToGroomProperty);

        #endregion

        public GroomShredderSnapshotBlobTimerTask(
            ICloudBlobClient blobClient,
            string type)
            : base(blobClient)
        {
            this._type = type;
        }
    }
}

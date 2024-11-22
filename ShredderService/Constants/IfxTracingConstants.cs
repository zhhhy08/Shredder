namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Constants
{
    /// <summary>
    /// IfxConstants
    /// </summary>
    internal class IfxTracingConstants
    {
        #region Metrics

        public const string ShredderBigFileMessageFailureMetric = "ShredderBigFileMessageFailure";

        public const string ShredderBigFileBlockCopyMessageFailureMetric = "ShredderBigFileBlockCopyMessageFailure";

        public const string ShredderBigFileAwaitCopyMessageFailureMetric = "ShredderBigFileAwaitCopyMessageFailure";

        public const string ShredderProcessingMessageFailureMetric = "ShredderProcessingMessageFailure";

        public const string ShredderSnapshotOriginalFileSizeMetric = "ShredderSnapshotOriginalFileSize";

        // This metric will calculate number of actual resources ingested in sailfish by Shredder
        public const string ShredderSnapshotNumberOfResourcesIngestedMetric = "ShredderSnapshotNumberOfResourcesIngested";

        // This metric will calculate number of resources that was originally present in Snapshot
        public const string ShredderSnapshotTotalNumberOfResourcesInSnapshotMetric = "ShredderSnapshotTotalNumberOfResourcesInSnapshot";

        public const string ShredderSnapshotToGroomMetric = "ShredderSnapshotToGroom";

        public const string ShredderSnapshotToGroomProperty = "ShredderSnapshotToGroom";

        #endregion
    }
}
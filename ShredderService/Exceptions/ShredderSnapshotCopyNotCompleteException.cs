namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Exceptions
{
    using System;

    public class ShredderSnapshotCopyNotCompleteException : Exception
    {
        public ShredderSnapshotCopyNotCompleteException(string message) : base(message)
        {
        }
    }
}
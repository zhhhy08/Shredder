namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService
{
    using Microsoft.ServiceFabric.Services.Runtime;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing.ActivityMonitoring;
    using Shared.Utilities;
    using System.Net;
    using System.Threading;
    using WindowsAzure.Governance.ResourcesCache.ActivityTracing;

    /// <summary>
    /// Project initialize
    /// </summary>
    internal static class Program
    {
        #region Tracing

        /// <summary>
        /// The shredder service main monitor factory
        /// </summary>
        private static readonly ActivityMonitorFactory ShredderServiceMainMonitorFactory =
            new ActivityMonitorFactory("ShredderService.Main");

        #endregion

        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main()
        {
            var methodMonitor = ShredderServiceMainMonitorFactory.ToMonitor();
            methodMonitor.TryCatchRethrow(() =>
            {
                // Set min threads
                ThreadPoolUtility.SetMinThreads(
                    1000, 1000, methodMonitor.Activity);
                ServicePointManager.UseNagleAlgorithm = false;
                ServicePointManager.Expect100Continue = false;
                ServicePointManager.CheckCertificateRevocationList = true;
                // Set the maximum number of concurrent connections 
                ServicePointManager.DefaultConnectionLimit = 1024;
                ServicePointManager.ReusePort = true;

                ServiceRuntime.RegisterServiceAsync("ShredderServiceType",
                    context => new ShredderService(context)).GetAwaiter().GetResult();
            });

            // Prevents this host process from terminating so services keep running.
            Thread.Sleep(Timeout.Infinite);
        }
    }
}

using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.AzureStorage;
using Microsoft.WindowsAzure.Governance.ResourcesCache.AzureStorageManagement.Contracts;
using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Configuration;
using Microsoft.WindowsAzure.Governance.ResourcesCache.WritePath.Common.Utils;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService.Utils
{
    internal static class ShredderUtils
    {
        public static (int delta, int numberOfMessagesInGroup) GetParametersToQueueMessagesOverPeriodOfTime(
            int maximumTimeToProcess,
            int totalMessagesForTheSnapshotJob)
        {
            /* We are going to scatter notifications over a period of time.
             * First we will find maximum time for the messages to be dispersed. Let's say it is m.
             * Then we find time difference between two messages. It is delta = 1 + (m / numberofmessages)
             * Then we find number of messages to be queued in a batch. It is numberOfMessagesInGroup = 1 + (numberofmessages / m)
            */

            var delta = 1 + (maximumTimeToProcess / totalMessagesForTheSnapshotJob);
            var numberOfMessagesInGroup = 1 + (totalMessagesForTheSnapshotJob / maximumTimeToProcess);

            return (delta, numberOfMessagesInGroup);
        }

        public static IList<ICloudStorageAccount> GetShredderStorageAccounts(IConfiguration configuration)
        {
            // Shredder is enabled right only in
            //     1. Proxy scale unit
            //     2. ArmDT scale unit
            //     3. Nrp Scale unit
            //     4. ARG scale unit (INT, DF and EUAP only). Shredder is not enabled in any other environment in ARG scale unit. 
            // Shredder storage accounts are there in
            //     1. Proxy scale unit
            //     2. ArmDT scale unit
            //     3. Nrp Scale unit
            //     4. ARG Int envirnoment. ARG DF and EUAP uses admin storage account.
            var adminStorageAccount = ChaosAzureStorageAccountFactory.Parse(
                   configuration,
                   ServiceConfigConstants.CommonSection, ServiceConfigConstants.AdminStorageAccountName,
                   nameof(ShredderUtils));

            var shredderStorageAccounts = new List<ICloudStorageAccount>() { adminStorageAccount };

            if (ScaleUnitUtil.AreShredderStorageAccountsPresentInScaleUnit())
            {
                shredderStorageAccounts = ChaosAzureStorageAccountFactory.ParseList(
                    configuration,
                    ServiceConfigConstants.CommonSection,
                    ServiceConfigConstants.ShredderStorageAccountNames,
                    ';',
                    nameof(ShredderUtils)).ToList();
            }

            return shredderStorageAccounts;
        }
    }
}
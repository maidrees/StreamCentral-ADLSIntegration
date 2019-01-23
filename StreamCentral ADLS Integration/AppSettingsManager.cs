using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace StreamCentral.ADLSIntegration
{
    class AppSettingsManager
    {

        //IMPORTANT: specify the name of Azure resource group here
        public static string resourceGroupName = ConfigurationManager.AppSettings["resourceGroupName"];// ConfigurationManager.AppSettings["resourceGroupName"];

        //IMPORTANT: the name of the data factory must be globally unique.
        public static string dataFactoryName = ConfigurationManager.AppSettings["dataFactoryName"];

        //IMPORTANT: specify the name of the source linked Service
        public static string linkedServiceNameSource = ConfigurationManager.AppSettings["linkedServiceNameSource"];

        //IMPORTANT: specify the name of the destination linked Service. These linked services have already been created in our SC - scenario.
        public static string linkedServiceNameDestination = ConfigurationManager.AppSettings["linkedServiceNameDestination"];

        //IMPORTANT: specify the CGS ADF Hub Name
        public static string cgsHubName = ConfigurationManager.AppSettings["cgsHubName"];

        //IMPORTANT: specify the name of the main container of the Destination Lake Store.
        public static string folderPath = ConfigurationManager.AppSettings["folderPath"];

        public static string subscriptionId = ConfigurationManager.AppSettings["SubscriptionId"];

        public static string resourceManagerEndPoint = ConfigurationManager.AppSettings["ResourceManagerEndpoint"];

        public static string activeDirectoryTenantID = ConfigurationManager.AppSettings["ActiveDirectoryTenantId"];

        public static string applicationId =ConfigurationManager.AppSettings["ApplicationId"];

        public static string password = ConfigurationManager.AppSettings["Password"];

        public static string windowsManagementUri = ConfigurationManager.AppSettings["WindowsManagementUri"];

        public static string activeDirectoryEndPoint = ConfigurationManager.AppSettings["ActiveDirectoryEndpoint"];
    }
}

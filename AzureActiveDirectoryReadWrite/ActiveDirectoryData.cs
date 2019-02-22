using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections;

using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure;


namespace AzureActiveDirectoryReadWrite
{
    class ActiveDirectoryData
    {

        static ActiveDirectoryData()
        {
          //  CreateManagementClientInstance();
        }

        //public static  CreateManagementClientInstance()
        //{
            

        //    //IMPORTANT: generate security token for the subsciption and AAD App
        //    TokenCloudCredentials aadTokenCredentials = new TokenCloudCredentials(ConfigurationManager.AppSettings["SubscriptionId"],
        //            GetAuthorizationHeader().Result);

        //    Uri resourceManagerUri = new Uri(ConfigurationManager.AppSettings["resourceManagerEndPoint"]);

        //    // create data factory management client
        //    //client = new DataFactoryManagementClient(aadTokenCredentials, resourceManagerUri);

        //    //return client;
        //}

        public static string GetClientToken()
        {
            string token = string.Empty;

            //IMPORTANT: generate security token for the subsciption and AAD App
            TokenCloudCredentials aadTokenCredentials = new TokenCloudCredentials(ConfigurationManager.AppSettings["subscriptionId"],
                    GetAuthorizationHeader().Result);

            return aadTokenCredentials.Token;
        }

        public static async Task<string> GetAuthorizationHeader()
        {
            AuthenticationContext context = new AuthenticationContext(ConfigurationManager.AppSettings["activeDirectoryEndPoint"] + ConfigurationManager.AppSettings["activeDirectoryTenantID"]);
            ClientCredential credential = new ClientCredential(
                ConfigurationManager.AppSettings["applicationId"],
                ConfigurationManager.AppSettings["password"]);
            AuthenticationResult result = await context.AcquireTokenAsync(
                resource: ConfigurationManager.AppSettings["windowsManagementUri"],
                clientCredential: credential);

            if (result != null)
                return result.AccessToken;

            throw new InvalidOperationException("Failed to acquire token");
        }
    }
}

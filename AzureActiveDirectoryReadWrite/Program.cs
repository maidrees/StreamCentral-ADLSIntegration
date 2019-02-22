using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Configuration;
namespace AzureActiveDirectoryReadWrite
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {

                string token = ActiveDirectoryData.GetClientToken();
                var tokenone = ActiveDirectoryData.GetAuthorizationHeader();

                // https://graph.windows.net//users?api-version

                WebClient client = new WebClient();
                //authentication using the Azure AD application
                //var token = GetAToken();
                client.Headers.Add(HttpRequestHeader.Authorization, "Bearer " + token);
                client.Headers.Add("Content-Type", "application/json");
                string uri = "https://graph.windows.net/" + ConfigurationManager.AppSettings["activeDirectoryTenantID"] + "/users/L1_admas@careysplc.co.uk/$links/manager?api-version=1.6";
                byte[] userdata = client.DownloadData(uri);

                var str = System.Text.Encoding.Default.GetString(userdata);

                string s = System.Text.Encoding.UTF8.GetString(userdata, 0, userdata.Length);


                char[] chars = new char[userdata.Length / sizeof(char)];
                System.Buffer.BlockCopy(userdata, 0, chars, 0, userdata.Length);
                string cstr = new string(chars);


                var table = (Encoding.Default.GetString(
                                 userdata,
                                 0,
                                 userdata.Length - 1)).Split(new string[] { "\r\n", "\r", "\n" },
                                                             StringSplitOptions.None);
            } catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            //POST to API
            //var result = client.UploadString(URI, body);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Configuration;

using Microsoft.Azure.Management.DataFactory;
using Microsoft.Rest;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Rest.Azure.Authentication;


namespace AzureDatalakeStorereader
{
    class ADLSGen1FileOperations
    {
       public static AdlsClient client;

        static ADLSGen1FileOperations()
        {
            var applicationId = ConfigurationManager.AppSettings["ApplicationId"];// "b93a619a-d985-49e4-a3f4-b9e97f958bb5";
            var secretKey = ConfigurationManager.AppSettings["Password"];//"cAxb4+mSLXfMZhulLbm5+t9Jx65z1TkR3MH8yv1pEMU=";
            var tenantId = ConfigurationManager.AppSettings["ActiveDirectoryTenantId"];//"90fa383b-7959-47d2-8c3a-ece9d79acd2b";
            var adlsAccountName = ConfigurationManager.AppSettings["adlsName"] + ".azuredatalakestore.net";
            var creds = ApplicationTokenProvider.LoginSilentAsync(tenantId, applicationId, secretKey).Result;

            client = AdlsClient.CreateClient(adlsAccountName, creds);

        }

        public static void DeleteEmptyFilesInFolder(string folderName, string searchText, bool isEmptyFilesOnly)
        {
            Dictionary<string, string> lstFilesInfolder = EnumerateDirectory(folderName, searchText);

            foreach (string fileName in lstFilesInfolder.Values)
            {
                if (isEmptyFilesOnly)
                {
                    if (GetFileProperties(fileName) <= 3)
                    {
                        Console.WriteLine("Found an Empty File: " + fileName);

                        DeleteFiles(fileName);
                    }
                }
                else
                {
                    DeleteFiles(fileName);
                }
            }
        }


        public static void DeleteFiles(string fileName)
        {
            try
            {
                System.Threading.CancellationToken cancelToken;

                client.Delete(fileName, cancelToken);

                Console.WriteLine("deleting the file now : " + fileName);
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public static void CreateFile(string fileName)
        {

            // Create a file - automatically creates any parent directories that don't exist
            // The AdlsOuputStream preserves record boundaries - it does not break records while writing to the store
            using (var stream = client.CreateFile(fileName, IfExists.Overwrite))
            {
                byte[] textByteArray = Encoding.UTF8.GetBytes("This is test data to write.\r\n");
                stream.Write(textByteArray, 0, textByteArray.Length);

                textByteArray = Encoding.UTF8.GetBytes("This is the second line.\r\n");
                stream.Write(textByteArray, 0, textByteArray.Length);
            }
        }

        public static void CreateFile(string fileName, StreamReader fileContent)
        {

            // Create a file - automatically creates any parent directories that don't exist
            // The AdlsOuputStream preserves record boundaries - it does not break records while writing to the store
            using (var stream = client.CreateFile(fileName, IfExists.Overwrite))
            {
                stream.Write(Encoding.UTF8.GetBytes(fileContent.ReadToEnd()), 0, 10);

            }
        }

        public static void AppendToFile(string fileName)
        {
            // Append to existing file
            using (var stream = client.GetAppendStream(fileName))
            {
                byte[] textByteArray = Encoding.UTF8.GetBytes("This is the added line.\r\n");
                stream.Write(textByteArray, 0, textByteArray.Length);
            }
        }

        public static StreamReader ReadFromFile(string fileName)
        {
            StreamReader readStream;
            //Read file contents
            using (readStream = new StreamReader(client.GetReadStream(fileName)))
            {
                //string line;
                //while ((line = readStream.ReadLine()) != null)
                //{
                //    Console.WriteLine(line);
                //}
            }

            return readStream;
        }

        public static long GetFileProperties(string fileName)
        {
            // Get file properties
            var directoryEntry = client.GetDirectoryEntry(fileName);

            return directoryEntry.Length;

        }

        public static void RenameFile(string fileName)
        {
            // Rename a file
            string destFilePath = "/Test/testRenameDest3.txt";
            client.Rename(fileName, destFilePath, true);
        }

        public static Dictionary<string, string> EnumerateDirectory(string folderPath)
        {

            return EnumerateDirectory(folderPath, "");
        }

        public static Dictionary<string, string> EnumerateDirectory(string folderPath, string searchText)
        {

            Dictionary<string, string> lstItemsInDir = new Dictionary<string, string>();

            // Enumerate directory
            foreach (var entry in client.EnumerateDirectory(folderPath))
            {
                if ((!System.String.IsNullOrEmpty(searchText)) && (entry.FullName.Contains(searchText)))
                {
                    lstItemsInDir.Add(entry.Name, entry.FullName);
                }
                else
                {
                    lstItemsInDir.Add(entry.Name, entry.FullName);
                }
            }
            return lstItemsInDir;
        }

        public static void CopyFileFromSourceToDest(string sourcePath, string destPath)
        {

            try
            {
                Dictionary<string, string> lstFilesInfolder = EnumerateDirectory(sourcePath);

                foreach (var fileName in lstFilesInfolder)
                {

                    using (var readStream = new StreamReader(client.GetReadStream(fileName.Value)))
                    {
                        string line = readStream.ReadToEnd();

                        if (!System.String.IsNullOrEmpty(line))
                        {
                            using (var stream = client.CreateFile(destPath + fileName.Key, IfExists.Overwrite))
                            {
                                byte[] txtArray = Encoding.UTF8.GetBytes(line);
                                stream.Write(txtArray, 0, txtArray.Length);
                            }
                        }

                    }
                }
                Console.WriteLine("Copied the directory");
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

    }
}

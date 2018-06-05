using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{
    class Program
    {
        private static string _dataSourceName = String.Empty;

        private static string _tableName = String.Empty;

        private static string _folderPath = String.Empty;

        private static string _filterDateTimeField = String.Empty;

        private static string _filterDateTimeInterval = String.Empty;

        static void Main(string[] args)
        {
            try
            {
                //Call Method: Create Data Sets, Pipelines for all structures qualified for criteria.
                ADFOperations.DeployADFDataSetsAndPipelines(_dataSourceName,
                    _tableName, _folderPath, _filterDateTimeField, _filterDateTimeInterval);
            }
            catch(Exception ex)
            {
                Console.WriteLine("Some exception occured: {0}", ex.Message);
                Console.ReadLine();
            }
        }

        static void LoadCommandLineArgs(String[] args)
        {
            try
            {
                String[] listArguments = args;

                try
                {
                    if (!System.String.IsNullOrEmpty(listArguments[1]))
                    {
                        _dataSourceName = listArguments[1].ToString();
                        Console.WriteLine(_dataSourceName);
                    }
                }catch(IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");                    
                }

                if (!System.String.IsNullOrEmpty(listArguments[2]))
                {
                    _tableName = listArguments[2].ToString();
                    Console.WriteLine(_tableName);
                }
                
                if (!System.String.IsNullOrEmpty(listArguments[3]))
                {
                    _folderPath = listArguments[3].ToString();
                    Console.WriteLine(_folderPath);
                }
                if (!System.String.IsNullOrEmpty(listArguments[4]))
                {
                    _filterDateTimeField = listArguments[4].ToString();
                    Console.WriteLine(_filterDateTimeField);
                }
                if (!System.String.IsNullOrEmpty(listArguments[5]))
                {
                    _filterDateTimeInterval = listArguments[5].ToString();
                    Console.WriteLine(_filterDateTimeInterval);
                }
            }
            catch (IndexOutOfRangeException ex)
            {
                Console.WriteLine("Few arguments are not provided : {0}", ex.Message);
                Console.ReadLine();
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{
    class Program
    {
        private static string dataSourceName = String.Empty;

        private static string tableName = String.Empty;

        private static string folderPath = String.Empty;

        private static string filterDateTimeField = String.Empty;

        private static string filterDateTimeInterval = String.Empty;

        static void Main(string[] args)
        {
            try
            {
                try
                {
                    String[] arguments = Environment.GetCommandLineArgs();

                    if (!System.String.IsNullOrEmpty(arguments[1]))
                    {
                        dataSourceName = arguments[0].ToString();
                        Console.WriteLine(dataSourceName);
                    }

                    if (!System.String.IsNullOrEmpty(arguments[2]))
                    {
                        tableName = arguments[1].ToString();
                        Console.WriteLine(tableName);
                    }
                    if (!System.String.IsNullOrEmpty(arguments[3]))
                    {
                        folderPath = arguments[2].ToString();
                        Console.WriteLine(folderPath);
                    }
                    if (!System.String.IsNullOrEmpty(arguments[4]))
                    {
                        filterDateTimeField = arguments[3].ToString();
                        Console.WriteLine(filterDateTimeField);
                    }
                    if (!System.String.IsNullOrEmpty(arguments[5]))
                    {
                        filterDateTimeInterval = arguments[5].ToString();
                        Console.WriteLine(filterDateTimeInterval);
                    }
                }
                catch(IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Few arguments are not provided : {0}", ex.Message);
                    Console.ReadLine();
                }

                 ADFOperations.DeployADFDataSetsAndPipelines();
            }
            catch(Exception ex)
            {
                Console.WriteLine("Some exception occured: {0}", ex.Message);
                Console.ReadLine();
            }
        }
    }
}

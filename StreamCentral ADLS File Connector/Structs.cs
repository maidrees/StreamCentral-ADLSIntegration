using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace AzureDatalakeStorereader
{
    public static class DataSourceDetails
    {
        private static JObject _inputJson;

        private static JArray _sources;

        public static JObject InputJson
        {
            get
            {
                if(_inputJson != null)
                return _inputJson;
                else
                {
                    _inputJson = JObject.Parse("{'INPUT':{'TransType':'UpdateADLSFilesModifiedDate','VID':34,'LoginId':1619,'filter':'all','pagesize':10,'pagenumber':0,'lastid':0}}");
                    return _inputJson;
                }
            }
            set
            {
                _inputJson = value;
            }

        }

    public static JArray Sources
        {
            get
            { return _sources; }
            set
            { _sources = value; }
        }

        public static void LoadDataSourceObjects()
        {

        }

        public static string ExecuteDataSourceDetails()
        {
            string opResult = String.Empty;

            Logging.WriteToLog(LoggerEnum.INFO, "ExecuteDataSourceDetails Start time : " + DateTime.Now, Constants.LogFileInputPrefix);
            try
            {
                string query = "select usp_adla('" + DataSourceDetails.InputJson + "')";
             
                object objResult = MasterDBUtils.GenerateSQLQueryCommand(query).ExecuteScalar();
           
                opResult = objResult.ToString();
            }
            catch(Npgsql.NpgsqlException ex)
            {

            }
            catch(Exception ex)
            {

            }

            return opResult;

        }

        internal static void LoadDataSourceObjects(JObject jOpAllSourcesData)
        {         
            try
            {
                DataSourceDetails.Sources = (JArray)jOpAllSourcesData["SPOKE"];      
                
            }
            catch(Exception ex)
            {
                Console.WriteLine("exception occured in loading the data source main object:" + ex.Message);
            }            
        }
    }

    public static class DataSource
    {
        private static string _dataSourceName;
        
        private static string _tableName;

        private static string _dataSourceID;

        private static string _adlaProvName;

        private static string _adlaType;

        private static string _folderPath;

        private static string _containerPath;

        private static string _frequency;

        private static string _frequencyUOM;

        private static string _runsAt;

        private static DateTime _adlaProvCreatedDate;

        private static DateTime _adlaProvModifiedDate;

        private static DateTime _dsCreatedDate;

        private static DateTime _dsLastIteratedDate;

        private static string _adlaProvId;

        private static string _engineInstanceId;

       

        public static string DataSourceName
        {
            get
            {
                return _dataSourceName;
            }
            set
            {
                _dataSourceName = value;
            }
        }

        public static string TableName
        {
            get
            {
                return _tableName;
            }
            set
            {
                _tableName = value;
            }
        }

        public static string DataSourceID
        {
            get
            {
                return _dataSourceID;
            }
            set
            {
                _dataSourceID = value;
            }
        }

        public static string ADLAProvisioningName
        {
            get { return _adlaProvName; }
            set { _adlaProvName = value; }
        }

        public static string ADLAType
        {
            get { return _adlaType; }
            set
            {
                _adlaType = value;
            }
        }
        
        public static string FolderPath
        {
            get { return _folderPath; }
            set { _folderPath = value; }
        }

        public static string Frequency
        {
            get { return _frequency; }
            set { _frequency = value; }
        }

        public static string FrequencyUOM
        {
            get { return _frequencyUOM; }
            set { _frequencyUOM = value; }
        }


        public static string ContainerPath
        {
            get { return _containerPath; }
            set { _containerPath = value; }
        }

        public static string RunsAt
        {
            get { return _runsAt; }
            set { _runsAt = value; }
        }

        public static DateTime ADLAProvCreatedDate
        {
            get { return _adlaProvCreatedDate; }
            set {
                if (value == null)
                {
                    _adlaProvCreatedDate = DateTime.Now.AddYears(10);
                }
                else
                {
                    _adlaProvCreatedDate = value;
                }
            }
        }

        public static DateTime ADLAProvModifiedDate
        {
            get { return _adlaProvModifiedDate; }
            set { _adlaProvModifiedDate = value; }
        }

        public static DateTime DSCreatedDate
        {
            get { return _dsCreatedDate; }
            set {
                if (value == null)
                {
                    _dsCreatedDate = DateTime.Now.AddYears(10);
                }
                else
                {
                    _dsCreatedDate = value;
                }
            }
        }

        public static DateTime DSLastIteratedDate
        {
            get { return _dsLastIteratedDate; }
            set
            {
                if (value == null)
                {
                    _dsLastIteratedDate = DateTime.Now.AddYears(10);
                }
                else
                {
                    _dsLastIteratedDate = value;
                }

            }
        }    
        
        public static string ADLAProvId
        {
            get { return _adlaProvId; }
            set
            {
                _adlaProvId = value;
            }
        }

        public static string EngineInstanceId
        {
            get { return _engineInstanceId; }
            set
            {
                _engineInstanceId = value;
            }
        }
    }

}

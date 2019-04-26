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
                    _inputJson = JObject.Parse("{'INPUT':{'TransType':'dashboard','VID':34,'LoginId':1619,'filter':'all','pagesize':10,'pagenumber':0,'lastid':0}}");
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
            string query = "select usp_dbservgetsourcecommonurl('" + DataSourceDetails.InputJson + "')";

            object objResult = MasterDBUtils.GenerateSQLQueryCommand(query).ExecuteScalar();
            string strresult = objResult.ToString();

            return strresult;

        }

        internal static JArray LoadDataSourceObjects(JObject jOpAllSourcesData)
        {
            DataSourceDetails.Sources = (JArray) jOpAllSourcesData["SPOKE"];

            return Sources;
        }
    }

    public static class DataSource
    {
        private static JObject _engines;


    }
}

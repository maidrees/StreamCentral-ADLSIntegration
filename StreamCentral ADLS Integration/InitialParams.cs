using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{
    public static class InitialParams
    {
        private static string _filterDistinctField = string.Empty;

        private static string _dataSourceName = String.Empty;

        private static string _tableName = String.Empty;

        public static string _folderPath = String.Empty;

        public static string _filterDateTimeField = String.Empty;

        public static string _filterDateTimeInterval = String.Empty;

        public static string _deployCriteria = String.Empty;

        public static string[] _searchText;

        public static string FilterDistinctField
        {
            get
            { return _filterDateTimeField; }
            set
            { _filterDateTimeField = value; }
        }

        public static string DataSourceName
        {
            get { return _dataSourceName; }
            set { _dataSourceName = value; }
        }

        public static string TableName
        {
            get { return _tableName; }
            set { _tableName = value; }
        }

        public static string FolderPath
        {
            get { return _folderPath; }
            set { _folderPath = value; }
        }

        public static string FilterDateTimeField
        {
            get { return _filterDateTimeField; }
            set { _filterDateTimeField = value; }
        }

        public static string FilterDateTimeInterval
        {
            get { return _filterDateTimeInterval; }
            set { _filterDateTimeInterval = value; }
        }

        public static string DeployCriteria
        {
            get { return _deployCriteria; }
            set { _deployCriteria = value; }
        }

        public static string[] SearchText
        {
            get { return _searchText; }
            set { _searchText = value; }
        }
    }

}

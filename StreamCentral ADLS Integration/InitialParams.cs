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

        private static string _folderPath = String.Empty;

        private static string _dataSourcePathInADLS = String.Empty;

        private static string _tablePathInADLS = String.Empty;

        private static string _filterDateTimeField = String.Empty;

        private static string _filterDateTimeInterval = String.Empty;

        private static string _deployCriteria = String.Empty;

        private static string _searchText01 = String.Empty;

        private static string _searchText02 = String.Empty;

        private static string _searchText03 = String.Empty;

        private static string _primaryKey = String.Empty;

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

        public static string SearchText01
        {
            get { return _searchText01; }
            set { _searchText01 = value; }
        }
        
        public static string SearchText02
        {
            get { return _searchText02; }
            set { _searchText02 = value; }
        }
        
        public static string SearchText03
        {
            get { return _searchText03; }
            set { _searchText03 = value; }
        }

        public static string PrimaryKey
        {
            get { return _primaryKey; }
            set { _primaryKey = value;  }
        }

        public static string DataSourcePathInADLS
        {
            get { return _dataSourcePathInADLS; }
            set { _dataSourcePathInADLS = value; }
        }

        public static string TablePathInADLS
        {
            get { return _tablePathInADLS; }
            set { _tablePathInADLS = value; }
        }
    }

}

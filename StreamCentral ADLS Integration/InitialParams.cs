﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

using Microsoft.Azure;
using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Common.Models;

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

        private static Frequency _actFrequencyType;

        private static string _actFrequencyInterval = String.Empty;

        private static string _deployCriteria = String.Empty;

        private static string _searchText01 = String.Empty;

        private static string _searchText02 = String.Empty;

        private static string _searchText03 = String.Empty;

        private static string _primaryKey = String.Empty;

        private static string _environment = String.Empty;

        private static CopyOnPremSQLToADLAType _copyOnPremToADLAType;

        private static string _offsetIntervalOfDataSlice = "0";

        private static string _delayIntervalOfActivity = "0";

        private static SliceType _sliceType = SliceType.End;

        private static string _tempcompprefix = String.Empty;

        private static string _temppathdeviation = String.Empty;

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
            get
            {
                try
                {
                    return (String.IsNullOrEmpty(_folderPath) ? ConfigurationSettings.AppSettings["folderPath"] : _folderPath);
                }
                catch(Exception ex)
                {
                    return String.Empty;
                }
            }
            set { _folderPath = value; }
        }

        public static string FilterDateTimeField
        {
            get { return _filterDateTimeField; }
            set { _filterDateTimeField = value; }
        }

        public static Frequency ActivityFrequencyType
        {
            get { return _actFrequencyType; }
            set { _actFrequencyType = value; }
        }

        public static string ActivityFrequencyInterval
        {
            get { return _actFrequencyInterval; }
            set
            {
                if (!String.IsNullOrEmpty(value))
                {
                    _actFrequencyInterval = value;
                }
                else
                {
                    _actFrequencyInterval = "1";
                }
            }
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
            get
            {

                return (String.IsNullOrEmpty(_dataSourcePathInADLS)) ? Utils.GetdataSourceType(TableName) : _dataSourcePathInADLS;               
            }
            set
            {
                if (String.IsNullOrEmpty(value))
                {
                    _dataSourceName = Utils.GetdataSourceType(_dataSourceName);
                }
                else
                {
                    _dataSourcePathInADLS = value;
                }
            }
        }

        public static string TablePathInADLS
        {
            get
            {
                //return  (String.IsNullOrEmpty(_tablePathInADLS)) ?  Utils.GetFormattedFolderPath(_tableName) : _tablePathInADLS;
                return _tablePathInADLS;
            }
            set
            {
                if (String.IsNullOrEmpty(value))
                {
                    _tablePathInADLS = Utils.GetFormattedFolderPath(_tableName);
                }
                else
                {
                    _tablePathInADLS = value;
                }

            }
        }

        public static CopyOnPremSQLToADLAType OnPremiseADLAType
        {
            get
            {
                return _copyOnPremToADLAType;
            }
            set
            {
                _copyOnPremToADLAType = value;
            }                
        }

        public static string Environment
        {
            get { return _environment; }
            set { _environment = value; }
        }

        public static string OffsetIntervalOfDataSlice
        {
            get { return _offsetIntervalOfDataSlice; }
            set {
                if (!String.IsNullOrEmpty(value))
                {
                    _offsetIntervalOfDataSlice = value;
                }
                else
                {
                    _offsetIntervalOfDataSlice = "0";
                }
            }
        }

        public static string DelayIntervalOfActivity
        {
            get { return _delayIntervalOfActivity;  }
            set
            {
                if (!String.IsNullOrEmpty(value))
                {
                    _delayIntervalOfActivity = value;
                }
                else
                {
                    _delayIntervalOfActivity = "0";
                }

            }
        }

        public static string TempPathDeviation
        {
            get { return _temppathdeviation; }
            set
            {
                _temppathdeviation = value;
            }
        }

        public static string TempCompPrefix
        {
            get { return _tempcompprefix; }
            set
            {
                _tempcompprefix = value;
            }
        }

        public static SliceType SliceType
        {
            get { return _sliceType;  }
            set
            {
                _sliceType = value;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{
    public enum CopyOnPremSQLToADLAType
    {
        Transactional,
        Distinct,
        All,
        LastIteration,
        Flattened,
        NinethDay,
        FifteenthDay
    }

    public enum IsPipelineStartModePaused
    {
        True,
        False
    }

    public enum DeploymentCriteria
    {
        ExactMatchSource,
        SearchTextMatchSource
    }

    public enum IsHeaderOrDataType
    {
        //This is for header deployment
        H,
        //This is for data deployment
        D
    }

    public enum Frequency
    {
        Year,
        Month,
        Day,
        Hour,
        Minute
    }

    public enum InputCommandLineArgs
    {
        criteria,
        datasource,
        table,
        folderpath,
        datefilter,
        dataslice,
        datasliceinterval,
        primarykey,
        copydataType,
        tablepathinadls,
        environment,
        offsetinterval,
        delayinterval,
        datasourcepathinadls,
        slicetype,
        temppathdeviation,
        tempcompprefix,
        pipelinename,
        inputds,
        outputds,
        sourcetype,
        sourceStructureType
    }

    public enum SliceType
    {
        Start,
        End
    }

    public enum EnumSourceType
    {
        OnPremiseSQLServer,
        AzureSQLServer
    }

    public enum EnumDeleteSearchType
    {
        Exact,
        StartWith,
        Contains            
    }

    public enum EnumSourceStructureType
    {
        Table,
        StoredProc,
        TableView
    }
}
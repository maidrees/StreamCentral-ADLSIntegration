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
        Flattened
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

}

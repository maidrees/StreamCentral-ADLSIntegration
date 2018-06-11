using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{
    class EnumDefinitions
    {
    }

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

}

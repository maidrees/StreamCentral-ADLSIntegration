using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{
    class Constants
    {
        public static readonly string _inputDSHeaderNameUnformatted = "SC_DSI_H_{0}_{1}_{2}";

        public static readonly string _inputDSDataNameUnformatted = "SC_DSI_D_{0}_{1}_{2}";

        public static readonly string _outputDSHeaderNameUnformatted = "SC_DSO_H_{0}_{1}_{2}";

        public static readonly string _outputDSDataNameUnformatted = "SC_DSO_D_{0}_{1}_{2}";

        public static readonly string _actHeaderNameUnformatted = "SC_ACT_H_{0}_{1}_{2}";

        public static readonly string _actDataNameUnformatted = "SC_ACT_D_{0}_{1}_{2}";

        public static readonly string _pipelineHeaderNameUnformatted = "SC_PL_H_{0}_{1}_{2}";

        public static readonly string _pipelineDataNameUnformatted = "SC_PL_D_{0}_{1}{2}";

        public static readonly string _headerFileNameUnformatted = "Header_{0}";

        public static readonly string _dataFileNameUnformatted = "Data_{0}";

        public static readonly string _dataFileNameTransactionalUnformatted = "Data_{0}-{year}-{month}-{day}-{hour}-{minute}";
    
    }
}

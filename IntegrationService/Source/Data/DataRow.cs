using System;
using System.Collections.Generic;

namespace SwarmDL.Data
{
    public record DataRow
    {
        public IReadOnlyList<double> Input { get; init; }
        public IReadOnlyList<double> Output { get; init; }
    }
}
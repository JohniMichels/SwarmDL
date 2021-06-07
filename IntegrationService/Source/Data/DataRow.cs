using System;

namespace SwarmDL.Data
{
    public record DataRow
    {
        public dynamic Input { get; init; }
        public dynamic Output { get; init; }
    }
}
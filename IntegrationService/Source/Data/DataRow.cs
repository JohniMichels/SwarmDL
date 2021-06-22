using System;
using System.Collections.Generic;
using Parquet.Data;

namespace SwarmDL.Data
{
    public record DataRow
    {
        public static Schema ParquetSchema { get; private set; } = new Schema(
            new DataField<IEnumerable<double>>("input"),
            new DataField<IEnumerable<double>>("output")
        );
        public IReadOnlyList<double> Input { get; init; }
        public IReadOnlyList<double> Output { get; init; }
    }
}
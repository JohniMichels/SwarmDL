using System.Reactive;
using System.Reactive.Linq;
using System;

namespace SwarmDL.Data
{
    public interface IDataProvider
    {
        IObservable<DataRow> GetData(string query);
    }
}
using System.Reactive;
using Amazon.Runtime;
using Amazon.Athena;
using System.Reactive.Linq;
using System;
using Microsoft.Extensions.Configuration;
using Amazon.Athena.Model;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;

[assembly: InternalsVisibleTo("IntegrationServiceTests")]
namespace SwarmDL.Data
{
    public class AWSAthenaOptions
    {
        public const string Section = "AWSAthena";
        public string WorkGroup { get; set; } = "primary";

    }
    public class AWSAthenaDataProvider : IDataProvider
    {
        private AmazonAthenaClient Client { get; set; }
        private IOptions<AWSAthenaOptions> Options { get; set; }
        private ILogger<AWSAthenaDataProvider> Logger { get; set; }

        public AWSAthenaDataProvider(
            AmazonAthenaClient client,
            IOptions<AWSAthenaOptions> options,
            ILogger<AWSAthenaDataProvider> logger
        )
        {
            Client = client;
            Options = options;
            Logger = logger;
        }

        internal void LogQueryExecution(QueryExecution queryExecution)
        {
            var queuedValue = QueryExecutionState.QUEUED.Value;
            var runningValue = QueryExecutionState.RUNNING.Value;
            var cancelledValue = QueryExecutionState.CANCELLED.Value;
            var failedValue = QueryExecutionState.FAILED.Value;
            var successValue = QueryExecutionState.SUCCEEDED.Value;
            var logSwitch = new Dictionary<string, Action>
            {
                {
                    queuedValue,
                    () => Logger.LogInformation("Query {QueryStatus} for execution", queuedValue)
                },
                {
                    runningValue,
                    () => Logger.LogInformation("Query is {QueryStatus}. {DataRead} bytes scanned", runningValue, queryExecution.Statistics.DataScannedInBytes)
                },
                {
                    cancelledValue,
                    () => Logger.LogError("Query was {QueryStatus}", cancelledValue)
                },
                {
                    failedValue,
                    () => Logger.LogError("Query {QueryStatus}. Error : {ErrorMessage}", failedValue, queryExecution.Status.StateChangeReason)
                },
                {
                    successValue,
                    () => Logger.LogInformation("Query {QueryStatus}. {DataRead} bytes scanned", successValue, queryExecution.Statistics.DataScannedInBytes)
                }
            };
            logSwitch[queryExecution.Status.State.Value]();
        }

        internal bool CheckQueryFinished(QueryExecution queryExecution) =>
            new[] { 
                QueryExecutionState.CANCELLED, 
                QueryExecutionState.FAILED, 
                QueryExecutionState.SUCCEEDED 
            }.Any(s => s.Value == queryExecution.Status.State.Value);

        internal IObservable<StartQueryExecutionResponse> StartQuery(string query)
        {
            return Observable.FromAsync(
                cancel =>
                Client.StartQueryExecutionAsync(
                    new StartQueryExecutionRequest()
                    {
                        QueryString = query,
                        WorkGroup = Options.Value.WorkGroup
                    },
                    cancel
                )
            );
        }

        internal IObservable<QueryExecution> PollForQueryResult(StartQueryExecutionResponse response)
        {
            return Observable.Repeat(
                Observable.FromAsync(
                    cancel => Client.GetQueryExecutionAsync(
                        new GetQueryExecutionRequest()
                        {
                            QueryExecutionId = response.QueryExecutionId
                        },
                        cancel
                    )
                ).Select(r => r.QueryExecution)
                .Do(LogQueryExecution)  
            )
            .Where(CheckQueryFinished)
            .FirstAsync();
        }
        internal IObservable<GetQueryResultsResponse> GetResultsFrom(QueryExecution queryExecution, string nextToken = null)
        {
            return Observable.FromAsync(
                cancel => Client.GetQueryResultsAsync(
                    new GetQueryResultsRequest()
                    {
                        QueryExecutionId = queryExecution.QueryExecutionId,
                        NextToken = nextToken
                    },
                    cancel
                )
            ).SelectMany(r =>
                Observable.Return(r)
                .Merge(
                    string.IsNullOrWhiteSpace(r.NextToken) ?
                    Observable.Empty<GetQueryResultsResponse>() :
                    GetResultsFrom(
                        queryExecution,
                        r.NextToken
                    )
                )
            );
        }
 
        public IObservable<DataRow> GetData(string query)
        {
            return StartQuery(query)
            .SelectMany(PollForQueryResult)
            .SelectMany(queryResult => GetResultsFrom(queryResult))
            .Select(result => result.ResultSet)
            .SelectMany(result =>
                result.Rows.Select(row =>
                    row.Data.Zip(
                        result.ResultSetMetadata.ColumnInfo,
                        (v, c) => (c.Type, c.Name, Value: v.VarCharValue)
                    )
                ).ToObservable()
            )
            .Select(r => new DataRow()
            {
                Input = r
            });
        }
    }
}
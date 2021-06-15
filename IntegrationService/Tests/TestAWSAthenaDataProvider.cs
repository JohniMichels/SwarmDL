using System;
using Xunit;
using Moq;
using Amazon.Athena;
using Microsoft.Extensions.Logging;
using SwarmDL.Data;
using Amazon.Runtime;
using Amazon;
using System.Linq;
using FluentAssertions;
using Microsoft.Extensions.Options;
using System.Reactive.Linq;
using Amazon.Athena.Model;
using System.Threading;
using Newtonsoft.Json;
using Xunit.Categories;

namespace IntegrationService.Tests
{
    public class TestAWSAthenaDataProvider
    {

        public TestAWSAthenaDataProvider()
        {
            MockClient = new Mock<AmazonAthenaClient>(
                FallbackCredentialsFactory.GetCredentials(true),
                new AmazonAthenaConfig(){RegionEndpoint = RegionEndpoint.USEast1}
            );
            MockLogger = new Mock<ILogger<AWSAthenaDataProvider>>();
            MockOptions = new Mock<IOptions<AWSAthenaOptions>>();

            MockOptions.SetupGet(o => o.Value).Returns(
                new AWSAthenaOptions()
                {
                    WorkGroup = "primary"
                }
            );
        }

        public Mock<AmazonAthenaClient> MockClient { get; set; }
        public Mock<IOptions<AWSAthenaOptions>> MockOptions { get; set; }
        public Mock<ILogger<AWSAthenaDataProvider>> MockLogger { get; set; }


        [Fact]
        [UnitTest]
        public void MockingWorks()
        {
            var provider = new AWSAthenaDataProvider(
                MockClient.Object,
                MockOptions.Object,
                MockLogger.Object
            );
            Assert.NotNull(provider);
        }

        [Fact]
        [UnitTest]
        public void SendsQueryRequest()
        {
            var provider = new AWSAthenaDataProvider(
                MockClient.Object,
                MockOptions.Object,
                MockLogger.Object
            );
            var results = provider.StartQuery("SELECT 1");
            results.Wait();
            // Client.StartQueryExecutionAsync(
            //MockClient.Setup(t => t.StartQueryExecutionAsync)
            MockClient.Verify(o => o.StartQueryExecutionAsync(
                It.Is<StartQueryExecutionRequest>(r => r.QueryString == "SELECT 1" && r.WorkGroup == "primary"),
                It.IsAny<CancellationToken>()
                ),
                Times.Once()
            );
        }

        [Fact]
        [UnitTest]
        public void PollsForQuery()
        {
            var provider = new AWSAthenaDataProvider(
                MockClient.Object,
                MockOptions.Object,
                MockLogger.Object
            );
            var mockedStartQueryResponse = new StartQueryExecutionResponse()
            {
                QueryExecutionId = "1234"
            };
            var resultStates = new[] {
                QueryExecutionState.QUEUED,
                QueryExecutionState.RUNNING,
                QueryExecutionState.SUCCEEDED
            };
            var callCount = 0;
            var results = resultStates.Select(
                s => new QueryExecutionStatus() { State = s }
            ).Select(
                s => new QueryExecution() { 
                    Status = s, 
                    Statistics = new QueryExecutionStatistics() {
                        DataScannedInBytes = 1
                    }}
            ).Select(
                s => new GetQueryExecutionResponse() { QueryExecution = s }
            ).ToList();
            MockClient.Setup(
                c => c.GetQueryExecutionAsync(
                    It.Is<GetQueryExecutionRequest>(r => r.QueryExecutionId == "1234"),
                    It.IsAny<CancellationToken>()
                )
            ).ReturnsAsync(() =>
                {
                    callCount++;
                    Thread.Sleep(1);
                    return callCount switch
                    {
                        2 => results[1],
                        3 => results[2],
                        _ => results[0]
                    };
                });
            
            provider
                .PollForQueryResult(mockedStartQueryResponse)
                .Wait()
                .Status.State.Value.Should()
                .Be(QueryExecutionState.SUCCEEDED.Value, "the first final status is SUCCEEDED");
            callCount.Should().Be(3, "there's 1 queued, 1 running and 1 success");
            resultStates
                .Select(s => s.Value)
                .ToList()
                .ForEach(s =>
                {
                    MockLogger.Verify(
                        logger => logger.Log(
                            It.Is<LogLevel>(level => level == LogLevel.Information),
                            It.IsAny<EventId>(),
                            It.Is<It.IsAnyType>((v, t) => v.ToString().Contains(s)),
                            It.IsAny<Exception>(),
                            It.IsAny<Func<It.IsAnyType, Exception, string>>()
                        )
                    );
                });
        }

        [Fact]
        [UnitTest]
        public void GetResultsFromQuery()
        {
            var queryExecution = new QueryExecution()
            {
                QueryExecutionId = "1234"
            };
            var provider = new AWSAthenaDataProvider(
                MockClient.Object,
                MockOptions.Object,
                MockLogger.Object
            );
            var callCount = 0;
            MockClient.Setup(
                client => client.GetQueryResultsAsync(
                    It.IsAny<GetQueryResultsRequest>(),
                    It.IsAny<CancellationToken>()
                )
            ).ReturnsAsync(
                () =>
                {
                    callCount++;
                    return callCount < 2 ?
                        new GetQueryResultsResponse()
                        {
                            NextToken = "nextToken"
                        } :
                        new GetQueryResultsResponse();
                }
            );

            var result = provider.GetResultsFrom(queryExecution).Wait();

            new [] {
                null,
                "nextToken"
            }.ToList()
            .ForEach(
                token => {
                    MockClient.Verify(
                        client => client.GetQueryResultsAsync(
                            It.Is<GetQueryResultsRequest>(
                                r => r.QueryExecutionId == "1234" && r.NextToken == token
                            ),
                            It.IsAny<CancellationToken>()
                        ),
                        Times.Once()
                    );
                }
            );
            callCount.Should().Be(2, "1 time with token, other without");
        }
    
        [SkippableFact(typeof(AmazonServiceException))]
        [IntegrationTest]
        public void CheckQuery()
        {
            AWSConfigs.RegionEndpoint = RegionEndpoint.USEast1;
            var provider = new AWSAthenaDataProvider(
                new AmazonAthenaClient(),
                MockOptions.Object,
                MockLogger.Object
            );

            var results = provider.GetData(
                "SELECT ARRAY[1.0, 3] input, ARRAY[2, 4.0] as output UNION ALL\n" +
                "SELECT ARRAY[6.2, 3.1] input, ARRAY[13.7, -2.5] as output");

            var rows = results.ToListObservable();
            rows.Should().HaveCount(2, "the query has two rows");
        }
    
    }
}

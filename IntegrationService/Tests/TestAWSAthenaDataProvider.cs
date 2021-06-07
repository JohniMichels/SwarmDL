using System;
using Xunit;
using Moq;
using Amazon.Athena;
using Microsoft.Extensions.Logging;
using SwarmDL.Data;
using Microsoft.Extensions.Configuration;
using Amazon.Runtime;
using Amazon;
using System.Linq;

using Microsoft.Extensions.Options;
using System.Reactive;
using System.Reactive.Linq;
using Amazon.Athena.Model;
using System.Threading;

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
                    WorkGroup = "testworkgroup"
                }
            );
        }

        public Mock<AmazonAthenaClient> MockClient { get; set; }
        public Mock<IOptions<AWSAthenaOptions>> MockOptions { get; set; }
        public Mock<ILogger<AWSAthenaDataProvider>> MockLogger { get; set; }


        [Fact]
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
        public void ProviderSendsQueryRequest()
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
                It.IsAny<StartQueryExecutionRequest>(),
                It.IsAny<CancellationToken>()
                ),
                Times.Once
            );
        }
    }
}

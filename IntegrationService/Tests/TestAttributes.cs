using Xunit.Categories;

namespace IntegrationService.Tests
{
    public class UnitTestAttribute: CategoryAttribute
    {
        public UnitTestAttribute(): base("Unit")
        {}
    }
    public class IntegrationTestAttribute: CategoryAttribute
    {
        public IntegrationTestAttribute(): base("Integration")
        {}
    }
}
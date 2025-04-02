using Cassandra;
using Testcontainers.Cassandra;

namespace BasicUsage.Tests;

public class UnitTest1 : IAsyncLifetime
{
    private readonly CassandraContainer _cassandraContainer = new CassandraBuilder().Build();

    [Fact]
    public void Test1()
    {
        // Given
        using var cluster = Cluster.Builder().WithConnectionString(_cassandraContainer.GetConnectionString()).Build();
        using var session = cluster.Connect();
        const string keyspace = "test_keyspace";
        const string table = "test_table";
        session.Execute($"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};");
        session.Execute($"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (id UUID PRIMARY KEY, name text);");
        var id = Guid.NewGuid();
        session.Execute($"INSERT INTO {keyspace}.{table} (id, name) VALUES ({id}, 'Test Name');");
        // When
        var result = session.Execute($"SELECT name FROM {keyspace}.{table} WHERE id = {id};");
        // Then
        Assert.NotNull(result);
        Assert.Single(result.GetRows());
    }

    public Task InitializeAsync()
    {
        return _cassandraContainer.StartAsync();
    }

    public Task DisposeAsync()
    {
        return _cassandraContainer.DisposeAsync().AsTask();
    }
}

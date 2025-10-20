using CabEtlSolution.DataAccess;
using CabEtlSolution.Services;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;

namespace CabEtlSolution;

internal class Program
{
    static void Main(string[] args)
    {
        IConfiguration config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("config.json", optional: false, reloadOnChange: true)
            .Build();
        
        string csvFile = config["CsvFilePath"];
        string dupFile = config["DuplicatesFilePath"];
        string conn = config["ConnectionString"];
        InitializeDatabase(conn);
        var reader = new CsvDataReader();
        var dt = reader.ReadData(csvFile);

        var processor = new CabDataProcessor();
        var processedDt = processor.Process(dt, out var duplicates);

        var writer = new SqlDataWriter();
        writer.WriteDuplicates(duplicates, dupFile);
        writer.WriteData(processedDt, conn);

        Console.WriteLine($"Rows inserted: {processedDt.Rows.Count}");
    }

    static void InitializeDatabase(string connectionString)
    {
        using var connection = new SqlConnection(connectionString.Replace("Database=CabData", "Database=master"));
        connection.Open();

        using var checkCmd = new SqlCommand("SELECT COUNT(*) FROM sys.databases WHERE name = 'CabData'", connection);
        int dbCount = (int)checkCmd.ExecuteScalar();

        if (dbCount == 0)
        {
            string sqlScript = File.ReadAllText("CabData.sql");
            using var createCmd = new SqlCommand(sqlScript, connection);
            createCmd.ExecuteNonQuery();
            sqlScript = File.ReadAllText("Create.sql");
            using var createCmd1 = new SqlCommand(sqlScript, connection);
            createCmd1.ExecuteNonQuery();
        }
    }
}
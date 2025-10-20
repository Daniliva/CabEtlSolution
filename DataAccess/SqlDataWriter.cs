using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using CsvHelper;

namespace CabEtlSolution.DataAccess;

public class SqlDataWriter : ISqlDataWriter
{
    public void WriteData(DataTable dt, string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        
        using var createTableCmd = new SqlCommand(
            @"CREATE TABLE #TempTrips (
                PickupDatetime DATETIME NOT NULL,
                DropoffDatetime DATETIME NOT NULL,
                PassengerCount INT NOT NULL,
                TripDistance DECIMAL(10,2) NOT NULL,
                StoreAndFwdFlag VARCHAR(3) NOT NULL,
                PULocationID INT NOT NULL,
                DOLocationID INT NOT NULL,
                FareAmount DECIMAL(10,2) NOT NULL,
                TipAmount DECIMAL(10,2) NOT NULL
            )", connection);
        createTableCmd.ExecuteNonQuery();
        
        using var bulkCopy = new SqlBulkCopy(connection);
        bulkCopy.DestinationTableName = "#TempTrips";
        bulkCopy.ColumnMappings.Add("PickupDatetime", "PickupDatetime");
        bulkCopy.ColumnMappings.Add("DropoffDatetime", "DropoffDatetime");
        bulkCopy.ColumnMappings.Add("PassengerCount", "PassengerCount");
        bulkCopy.ColumnMappings.Add("TripDistance", "TripDistance");
        bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "StoreAndFwdFlag");
        bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
        bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
        bulkCopy.ColumnMappings.Add("FareAmount", "FareAmount");
        bulkCopy.ColumnMappings.Add("TipAmount", "TipAmount");
        bulkCopy.WriteToServer(dt);
        
        using var mergeCmd = new SqlCommand(
            @"MERGE INTO Trips AS target
              USING #TempTrips AS source
              ON target.PickupDatetime = source.PickupDatetime
              AND target.DropoffDatetime = source.DropoffDatetime
              AND target.PassengerCount = source.PassengerCount
              WHEN NOT MATCHED THEN
              INSERT (PickupDatetime, DropoffDatetime, PassengerCount, TripDistance, StoreAndFwdFlag, PULocationID, DOLocationID, FareAmount, TipAmount)
              VALUES (source.PickupDatetime, source.DropoffDatetime, source.PassengerCount, source.TripDistance, source.StoreAndFwdFlag, source.PULocationID, source.DOLocationID, source.FareAmount, source.TipAmount);",
            connection);
        mergeCmd.ExecuteNonQuery();
        
        using var dropTableCmd = new SqlCommand("DROP TABLE #TempTrips", connection);
        dropTableCmd.ExecuteNonQuery();
    }

    public void WriteDuplicates(DataTable duplicates, string filePath)
    {
        using var writer = new StreamWriter(filePath);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        foreach (DataColumn column in duplicates.Columns)
        {
            csv.WriteField(column.ColumnName);
        }
        csv.NextRecord();
        foreach (DataRow row in duplicates.Rows)
        {
            for (var i = 0; i < duplicates.Columns.Count; i++)
            {
                csv.WriteField(row[i]);
            }
            csv.NextRecord();
        }
    }
}
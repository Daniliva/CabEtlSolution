using System.Data;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace CabEtlSolution.DataAccess;

public class CsvDataReader : ICsvDataReader
{
    public DataTable ReadData(string filePath)
    {
        var dt = new DataTable();
        dt.Columns.Add("PickupDatetime", typeof(DateTime));
        dt.Columns.Add("DropoffDatetime", typeof(DateTime));
        dt.Columns.Add("PassengerCount", typeof(int));
        dt.Columns.Add("TripDistance", typeof(decimal));
        dt.Columns.Add("StoreAndFwdFlag", typeof(string));
        dt.Columns.Add("PULocationID", typeof(int));
        dt.Columns.Add("DOLocationID", typeof(int));
        dt.Columns.Add("FareAmount", typeof(decimal));
        dt.Columns.Add("TipAmount", typeof(decimal));

        var config = new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = true };
        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, config);
        csv.Read();
        csv.ReadHeader();

        int rowNumber = 0;
        using var errorLog = new StreamWriter("error_log.txt", append: true);
        while (csv.Read())
        {
            rowNumber++;
            try
            {
                var row = dt.NewRow();
                row["PickupDatetime"] = DateTime.ParseExact(csv.GetField("tpep_pickup_datetime")?.Trim() ?? "", "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture, DateTimeStyles.None);
                row["DropoffDatetime"] = DateTime.ParseExact(csv.GetField("tpep_dropoff_datetime")?.Trim() ?? "", "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture, DateTimeStyles.None);
                row["PassengerCount"] = TryParseInt(csv.GetField("passenger_count"), rowNumber, errorLog);
                row["TripDistance"] = TryParseDecimal(csv.GetField("trip_distance"), rowNumber, errorLog);
                row["StoreAndFwdFlag"] = csv.GetField("store_and_fwd_flag")?.Trim() ?? "";
                row["PULocationID"] = TryParseInt(csv.GetField("PULocationID"), rowNumber, errorLog);
                row["DOLocationID"] = TryParseInt(csv.GetField("DOLocationID"), rowNumber, errorLog);
                row["FareAmount"] = TryParseDecimal(csv.GetField("fare_amount"), rowNumber, errorLog);
                row["TipAmount"] = TryParseDecimal(csv.GetField("tip_amount"), rowNumber, errorLog);

                // Skip row if critical fields are invalid
                if (row["PickupDatetime"] != DBNull.Value && row["DropoffDatetime"] != DBNull.Value &&
                    row["PassengerCount"] != DBNull.Value && row["PULocationID"] != DBNull.Value &&
                    row["DOLocationID"] != DBNull.Value)
                {
                    dt.Rows.Add(row);
                }
                else
                {
                    errorLog.WriteLine($"Row {rowNumber}: Skipped due to invalid critical fields.");
                }
            }
            catch (Exception ex)
            {
                errorLog.WriteLine($"Row {rowNumber}: Error - {ex.Message}");
            }
        }
        return dt;
    }

    private static int TryParseInt(string? value, int rowNumber, StreamWriter errorLog)
    {
        if (string.IsNullOrWhiteSpace(value) || !int.TryParse(value.Trim(), out var result))
        {
            errorLog.WriteLine($"Row {rowNumber}: Invalid integer value '{value}'");
            return 0; 
        }
        return result;
    }

    private static decimal TryParseDecimal(string? value, int rowNumber, StreamWriter errorLog)
    {
        if (string.IsNullOrWhiteSpace(value) || !decimal.TryParse(value.Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
        {
            errorLog.WriteLine($"Row {rowNumber}: Invalid decimal value '{value}'");
            return 0m; 
        }
        return result;
    }
}
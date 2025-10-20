using System.Data;

namespace CabEtlSolution.Services;

public class CabDataProcessor : IDataProcessor
{
    public DataTable Process(DataTable dt, out DataTable duplicates)
    {
        duplicates = dt.Clone();

        foreach (DataRow row in dt.Rows)
        {
            var flag = row["StoreAndFwdFlag"].ToString();
            row["StoreAndFwdFlag"] = flag == "N" ? "No" : flag == "Y" ? "Yes" : flag;
            row["PickupDatetime"] = ((DateTime)row["PickupDatetime"]).AddHours(5);
            row["DropoffDatetime"] = ((DateTime)row["DropoffDatetime"]).AddHours(5);
        }

        var seen = new HashSet<string>();
        var toRemove = new List<DataRow>();
        foreach (DataRow row in dt.Rows)
        {
            var key = $"{row["PickupDatetime"]}_{row["DropoffDatetime"]}_{row["PassengerCount"]}";
            if (!seen.Add(key))
            {
                duplicates.ImportRow(row);
                toRemove.Add(row);
            }
        }

        foreach (var row in toRemove)
        {
            dt.Rows.Remove(row);
        }

        return dt;
    }
}
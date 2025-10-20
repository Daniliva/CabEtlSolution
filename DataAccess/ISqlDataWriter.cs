using System.Data;

namespace CabEtlSolution.DataAccess;

public interface ISqlDataWriter
{
    void WriteData(DataTable dt, string connectionString);
    void WriteDuplicates(DataTable duplicates, string filePath);
}
namespace CabEtlSolution.DataAccess;

public interface ICsvDataReader
{
    System.Data.DataTable ReadData(string filePath);
}
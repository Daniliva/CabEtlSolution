using System.Data;

namespace CabEtlSolution.Services;

public interface IDataProcessor
{
    DataTable Process(DataTable dt, out DataTable duplicates);
}
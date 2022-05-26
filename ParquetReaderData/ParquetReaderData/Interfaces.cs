using System.Collections.Generic;

namespace ParquetReaderData
{
    public interface IDataBaseConnect
    {
        void GetConnection(string dbname, string collection);
    }

    public interface IDataBase
    {
        void Append(List<GtmCalcResult> Data);

        void Delete();

        void Find();

        void Rewrite();
    }
}
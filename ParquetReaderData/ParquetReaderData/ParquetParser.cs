using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet;
using Parquet.Data;



namespace ParquetReaderData
{
    public class ParquetParser
    {
        private static List<GtmCalcResult> DataList = new List<GtmCalcResult>();
        private static List<DataColumn> Cols = new List<DataColumn>();
        private static int N_cols = 0;
        private string path;

        public ParquetParser(string path)
        {
            this.path = path;
        }

        public List<DataColumn> ReturnData()
        {
            return Cols;
        }


        public void CollectData()
        {
            using (Stream fileStream = System.IO.File.OpenRead(path))
            {
                // open parquet file reader
                using (var parquetReader = new ParquetReader(fileStream))
                {
                    // get file schema (available straight after opening parquet reader)
                    // however, get only data fields as only they contain data values
                    DataField[] dataFields = parquetReader.Schema.GetDataFields();

                    N_cols = parquetReader.RowGroupCount;
                    // enumerate through row groups in this file
                    for (int i = 0; i < parquetReader.RowGroupCount; i++)
                    {
                        // create row group reader
                        using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                        {
                            // read all columns inside each row group (you have an option to read only
                            // required columns if you need to.
                            DataColumn[] columns = dataFields.Select(groupReader.ReadColumn).ToArray();
                            foreach (var x in columns)
                            {
                                Cols.Add(x);
                            }
                        }
                    }
                }
            }
        }
        public List<GtmCalcResult> FillStruct()
        {
            int id = Cols[0].Data.Length; 
            for (int i = 0; i < id; i++)
            {
                GtmCalcResult result = new GtmCalcResult();
                result.Id = i;
                result.CalendarYear = (int)Cols[0].Data.GetValue(i);
                result.IsAllPeriod = (int)Cols[1].Data.GetValue(i);
                result.MeasureId = (long)Cols[2].Data.GetValue(i);
                result.PeoVersionId = (long)Cols[3].Data.GetValue(i); 
                result.PlanningObject = (long)Cols[4].Data.GetValue(i); 
                result.Month01Value = (double)Cols[5].Data.GetValue(i);
                result.Month02Value = (double)Cols[6].Data.GetValue(i); 
                result.Month03Value = (double)Cols[7].Data.GetValue(i); 
                result.Month04Value = (double)Cols[8].Data.GetValue(i);
                result.Month05Value = (double)Cols[9].Data.GetValue(i); 
                result.Month06Value = (double)Cols[10].Data.GetValue(i); 
                result.Month07Value = (double)Cols[11].Data.GetValue(i);
                result.Month08Value = (double)Cols[12].Data.GetValue(i); 
                result.Month09Value = (double)Cols[13].Data.GetValue(i); 
                result.Month10Value = (double)Cols[14].Data.GetValue(i);
                result.Month11Value = (double)Cols[15].Data.GetValue(i); 
                result.Month12Value = (double)Cols[16].Data.GetValue(i); 
                result.YearValue = (double)Cols[17].Data.GetValue(i);
                DataList.Add(result);
            }

            return DataList;
        }
    }
}
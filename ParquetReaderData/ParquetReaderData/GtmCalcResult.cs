
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;


namespace ParquetReaderData
{
    public struct GtmCalcResult
    {
        
        /// <summary>
        /// Уникальный идентификатор. Составной первичный ключ
        /// </summary>
        ///public CalcResultId Id => new (CalendarYear, MeasureId, PeoVersionId);
        public double GetMonthValue(int month) => month switch
        {
            1 => Month01Value,
            2 => Month02Value,
            3 => Month03Value,
            4 => Month04Value,
            5 => Month05Value,
            6 => Month06Value,
            7 => Month07Value,
            8 => Month08Value,
            9 => Month09Value,
            10 => Month10Value,
            11 => Month11Value,
            12 => Month12Value,
            _ => throw new NotImplementedException()
        };

        public bool GetIsAllPeriod()
        {
            return IsAllPeriod == 1;
        }
        public long Id { get; set; }
        public int IsAllPeriod { get; set; }

        public double YearValue { get; set; }

        public long PeoVersionId { get; set; }

        public long MeasureId { get; set; }

        public int CalendarYear { get; set; }

        public long PlanningObject { get; set; }

        public double Month01Value { get; set; }
        public double Month02Value { get; set; }
        public double Month03Value { get; set; }
        public double Month04Value { get; set; }
        public double Month05Value { get; set; }
        public double Month06Value { get; set; }
        public double Month07Value { get; set; }
        public double Month08Value { get; set; }
        public double Month09Value { get; set; }
        public double Month10Value { get; set; }
        public double Month11Value { get; set; }
        public double Month12Value { get; set; }
        
    }
}
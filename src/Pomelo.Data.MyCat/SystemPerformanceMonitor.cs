// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if NET451
using System;
using System.Diagnostics;


namespace Pomelo.Data.MyCat
{
  internal class SystemPerformanceMonitor : PerformanceMonitor
  {
    private static PerformanceCounter procedureHardQueries;
    private static PerformanceCounter procedureSoftQueries;

    public SystemPerformanceMonitor(MyCatConnection connection) : base(connection)
    {
      string categoryName = Resources.PerfMonCategoryName;

      if (connection.Settings.UsePerformanceMonitor && procedureHardQueries == null)
      {
        try
        {
          procedureHardQueries = new PerformanceCounter(categoryName,
                                                        "HardProcedureQueries", false);
          procedureSoftQueries = new PerformanceCounter(categoryName,
                                                        "SoftProcedureQueries", false);
        }
        catch (Exception ex)
        {
          MyCatTrace.LogError(connection.ServerThread, ex.Message);
        }
      }
    }

#if DEBUG
    private void EnsurePerfCategoryExist()
    {
      CounterCreationDataCollection ccdc = new CounterCreationDataCollection();
      CounterCreationData ccd = new CounterCreationData();
      ccd.CounterType = PerformanceCounterType.NumberOfItems32;
      ccd.CounterName = "HardProcedureQueries";
      ccdc.Add(ccd);

      ccd = new CounterCreationData();
      ccd.CounterType = PerformanceCounterType.NumberOfItems32;
      ccd.CounterName = "SoftProcedureQueries";
      ccdc.Add(ccd);

      if (!PerformanceCounterCategory.Exists(Resources.PerfMonCategoryName))
        PerformanceCounterCategory.Create(Resources.PerfMonCategoryName, null, PerformanceCounterCategoryType.SingleInstance,ccdc);
    }
#endif

    public override void AddHardProcedureQuery()
    {
      if (!Connection.Settings.UsePerformanceMonitor ||
          procedureHardQueries == null) return;
      procedureHardQueries.Increment();
    }

    public override void AddSoftProcedureQuery()
    {
      if (!Connection.Settings.UsePerformanceMonitor ||
          procedureSoftQueries == null) return;
      procedureSoftQueries.Increment();
    }
  }
}
#endif

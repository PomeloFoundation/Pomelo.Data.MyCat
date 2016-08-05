// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System.ComponentModel;
using System.Data;
using System.Data.Common;
#if NET451
using System.Security;
using System.Security.Permissions;
#endif

namespace Pomelo.Data.MyCat
{
  //[ToolboxBitmap(typeof(MyCatConnection), "MyCatClient.resources.connection.bmp")]
  [DesignerCategory("Code")]
  // [ToolboxItem(true)]
  public partial class MyCatConnection : DbConnection, ICloneable
  {
        /// <summary>
        /// Returns schema information for the data source of this <see cref="DbConnection"/>. 
        /// </summary>
        /// <returns>A <see cref="DataTable"/> that contains schema information. </returns>
#if NET451
    public override DataTable GetSchema()
    {
      return GetSchema(null);
    }

    /// <summary>
    /// Returns schema information for the data source of this 
    /// <see cref="DbConnection"/> using the specified string for the schema name. 
    /// </summary>
    /// <param name="collectionName">Specifies the name of the schema to return. </param>
    /// <returns>A <see cref="DataTable"/> that contains schema information. </returns>
    public override DataTable GetSchema(string collectionName)
    {
      if (collectionName == null)
        collectionName = SchemaProvider.MetaCollection;

      return GetSchema(collectionName, null);
    }

    /// <summary>
    /// Returns schema information for the data source of this <see cref="DbConnection"/>
    /// using the specified string for the schema name and the specified string array 
    /// for the restriction values. 
    /// </summary>
    /// <param name="collectionName">Specifies the name of the schema to return.</param>
    /// <param name="restrictionValues">Specifies a set of restriction values for the requested schema.</param>
    /// <returns>A <see cref="DataTable"/> that contains schema information.</returns>
    public override DataTable GetSchema(string collectionName, string[] restrictionValues)
    {
      if (collectionName == null)
        collectionName = SchemaProvider.MetaCollection;

      string[] restrictions = schemaProvider.CleanRestrictions(restrictionValues);
      MyCatSchemaCollection c = schemaProvider.GetSchema(collectionName, restrictions);
      return c.AsDataTable();
    }
#endif

      protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
      {
            if (isolationLevel == System.Data.IsolationLevel.Unspecified)
                return BeginTransaction();
            return BeginTransaction(isolationLevel);
      }

    protected override DbCommand CreateDbCommand()
    {
      return CreateCommand();
    }

#if NET451
    /*partial void AssertPermissions()
    {
      // Security Asserts can only be done when the assemblies 
      // are put in the GAC as documented in 
      // http://msdn.microsoft.com/en-us/library/ff648665.aspx
      if (this.Settings.IncludeSecurityAsserts)
      {
        PermissionSet set = new PermissionSet(PermissionState.None);
        set.AddPermission(new MyCatClientPermission(ConnectionString));
        set.Demand();
        MyCatSecurityPermission.CreatePermissionSet(true).Assert(); 
      }
    }*/
#endif

#region IDisposeable

        protected override void Dispose(bool disposing)
    {
      if (State == ConnectionState.Open)
        Close();
      base.Dispose(disposing);
    }

#endregion
  }
}


using DataMigration.Common;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace DataMigration.DBMS.MSSQL
{
    /// <summary>
    /// 表外键信息
    /// </summary>
    internal class TableFK : TableInfo
    {
        public List<string> FKs { get; } = new List<string>(); // 外键指向表
    }

    /// <summary>
    /// 基于 DataTable 的追加数据脚本对象
    /// </summary>
    internal class AppendScript
    {
        public DataTable Data { get; set; }    // 数据
        public bool KeepIdentity { get; set; } // 保留自增值
    }

    /// <summary>
    /// 基于 MERGE INTO 语法的更新数据脚本对象
    /// </summary>
    internal class MergeScript
    {
        public string TableName { get; set; }  // 临表名称
        public string PrepareSQL { get; set; } // 准备 SQL（创建临表）
        public DataTable Data { get; set; }    // 数据
        public string MergeSQL { get; set; }   // 合并 SQL
        public string MergeSQL2 { get; set; }  // 合并 SQL，用于不支持 MERGE INTO 语法的版本
        public string CleanSQL { get; set; }   // 清理 SQL
    }

    /// <summary>
    /// 基于更新和插入 SQL 的更新数据脚本对象
    /// </summary>
    internal class UpdateScript
    {
        public string UpdateSQL { get; set; } // 更新 SQL
        public string InsertSQL { get; set; } // 插入 SQL
    }

    /// <summary>
    /// Microsoft SQL Server
    /// </summary>
    public class MSSQL : IDBMSAssistant, IDBMSReader, IDBMSWriter, IAssemblyLoader
    {
        private readonly SqlConnection conn = new SqlConnection();
        private SqlTransaction trans = null;
        private string errMsg = "";
        private string title = "MSSQL";

        public bool BeginTransaction()
        {
            try
            {
                trans = conn.BeginTransaction();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
        }

        public bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            // 有数据
            if (data.Read())
            {
                if (table.WriteMode == WriteModes.Append)
                    BuildScriptWithDataTable(table, data, filter, ref script);
                else
                    BuildScriptWithMergeSQL(table, data, filter, ref script);
                //BuildScriptWithUpdateSQL(table, data, filter, ref script);

                return true;
            }

            return false;
        }

        protected void BuildScriptWithDataTable(Table table, IDataWrapper data, IDataFilter filter,
            ref object script)
        {
            // 创建数据表，字段清单与目标表须一致
            DataTable dt = new DataTable();

            for (int i = 0; i < table.DestFields.Length; i++)
                dt.Columns.Add(table.DestFields[i], data.GetFieldType(i));

            // 给跳过字段名称增加一个前缀，这样字段映射后取值就为 NULL
            string[] fields = ModifyFields(table.DestFields, table.SkipFields, "!!!");

            data.MapFields(fields);

            // 添加第一笔记录，仍然使用正确的字段名
            DataRow row = dt.NewRow();

            for (int i = 0; i < table.DestFields.Length; i++)
                row[i] = filter.GetValue(data, i, table.DestFields[i]);

            dt.Rows.Add(row);

            // 添加后续记录，仍然使用正确的字段名
            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                row = dt.NewRow();

                for (int i = 0; i < table.DestFields.Length; i++)
                    row[i] = filter.GetValue(data, i, table.DestFields[i]);

                dt.Rows.Add(row);
                r++;
            }

            script = new AppendScript() { Data = dt, KeepIdentity = table.KeepIdentity };
        }

        protected void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            string destTable = ProcessTableName(table.DestName);
            string tmpTable = $"[{destTable.Substring(1, destTable.Length - 2)}_{Guid.NewGuid():N}]";
            string mergeSQL, mergeSQL2;

            BuildScriptWithDataTable(table, data, filter, ref script);

            if (Version.Parse(conn.ServerVersion).Major >= 10) // 2008 或更新版本
            {
                StringBuilder sb = new StringBuilder();
                string[] fields = ExcludeFields(ExcludeFields(table.DestFields, table.KeyFields), table.SkipFields);
                string field = ProcessFieldName(table.KeyFields[0]);

                sb.Append("MERGE INTO ").Append(destTable).Append(" A USING ").Append(tmpTable).Append(" B ON A.")
                    .Append(field).Append(" = B.").Append(field);
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    field = ProcessFieldName(table.KeyFields[i]);
                    sb.Append(" AND A.").Append(field).Append(" = B.").Append(field);
                }
                field = ProcessFieldName(fields[0]);
                sb.AppendLine().Append(" WHEN MATCHED THEN UPDATE SET A.").Append(field).Append(" = B.").Append(field);
                for (int i = 1; i < fields.Length; i++)
                {
                    field = ProcessFieldName(fields[i]);
                    sb.Append(", A.").Append(field).Append(" = B.").Append(field);
                }

                fields = ExcludeFields(table.DestFields, table.SkipFields);
                sb.AppendLine().Append(" WHEN NOT MATCHED THEN INSERT (").Append(ProcessFieldNames(fields))
                    .Append(") VALUES (").Append(ProcessFieldNames(fields, "B"));
                sb.Append(");"); // 语句以分号结尾

                mergeSQL = sb.ToString();
                mergeSQL2 = "";
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                string[] fields = ExcludeFields(ExcludeFields(table.DestFields, table.KeyFields), table.SkipFields);
                string field = ProcessFieldName(fields[0]);

                sb.Append("UPDATE ").Append(destTable).Append(" SET ").Append(field).Append(" = B.").Append(field);
                for (int i = 1; i < fields.Length; i++)
                {
                    field = ProcessFieldName(fields[i]);
                    sb.Append(", ").Append(field).Append(" = B.").Append(field);
                }
                field = ProcessFieldName(table.KeyFields[0]);
                sb.Append(" FROM ").Append(tmpTable).Append(" B WHERE ")
                    .Append(destTable).Append(".").Append(field).Append(" = B.").Append(field);
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    field = ProcessFieldName(table.KeyFields[i]);
                    sb.Append(" AND ").Append(destTable).Append(".").Append(field).Append(" = B.").Append(field);
                }

                mergeSQL = sb.ToString();

                fields = ExcludeFields(table.DestFields, table.SkipFields);
                sb.Length = 0;
                field = ProcessFieldName(table.KeyFields[0]);
                sb.Append("INSERT INTO ").Append(destTable).Append(" (").Append(ProcessFieldNames(fields))
                    .Append(") SELECT ").Append(ProcessFieldNames(fields, "A")).Append(" FROM ").Append(tmpTable)
                    .Append(" A LEFT JOIN ").Append(destTable).Append(" B ON A.").Append(field).Append(" = B.")
                    .Append(field);
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    field = ProcessFieldName(table.KeyFields[i]);
                    sb.Append(" AND A.").Append(field).Append(" = B.").Append(field);
                }
                sb.Append(" WHERE B.").Append(field).Append(" IS NULL");

                mergeSQL2 = sb.ToString();
            }

            script = new MergeScript()
            {
                TableName = tmpTable.Substring(1, tmpTable.Length - 2),
                PrepareSQL = $"SELECT * INTO {tmpTable} FROM {destTable} WHERE 1 = 0",
                Data = ((AppendScript)script).Data,
                MergeSQL = mergeSQL,
                MergeSQL2 = mergeSQL2,
                CleanSQL = $"DROP TABLE {tmpTable}"
            };
        }

        [Obsolete("此模式执行效率极低，请用 BuildScriptWithMergeSQL() 替代")]
        protected void BuildScriptWithUpdateSQL(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            UpdateScript rst = new UpdateScript();
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(ExcludeFields(table.DestFields, table.KeyFields), table.SkipFields);

            data.MapFields(fields);
            sb.Append("UPDATE ").Append(ProcessTableName(table.DestName)).Append(" SET ")
                .Append(ProcessFieldName(fields[0])).Append(" = ")
                .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(ProcessFieldName(fields[i])).Append(" = ")
                    .Append(GetFmtValue(filter.GetValue(data, i, fields[i])));

            data.MapFields(table.KeyFields);
            sb.Append(" WHERE ")
                .Append(ProcessFieldName(table.KeyFields[0])).Append(" = ")
                .Append(GetFmtValue(filter.GetValue(data, 0, table.KeyFields[0])));
            for (int i = 1; i < table.KeyFields.Length; i++)
                sb.Append(" AND ").Append(ProcessFieldName(table.KeyFields[i])).Append(" = ")
                    .Append(GetFmtValue(filter.GetValue(data, i, table.KeyFields[i])));

            rst.UpdateSQL = sb.ToString();

            fields = ExcludeFields(table.DestFields, table.SkipFields);
            data.MapFields(fields);
            sb.Length = 0;
            sb.Append("INSERT INTO ").Append(ProcessTableName(table.DestName)).Append(" (")
                .Append(ProcessFieldNames(fields)).Append(") VALUES (")
                .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            rst.InsertSQL = sb.ToString();

            script = rst;
        }

        private string BytesToStr(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder("0x");

            foreach (byte b in bytes)
                sb.Append(b.ToString("X2"));

            return sb.ToString();
        }

        public void Close()
        {
            try
            {
                conn.Close();
            }
            catch { }
        }

        public bool CommitTransaction()
        {
            try
            {
                trans.Commit();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }

        public bool Connect(Database db)
        {
            string encrypt = db.Encrypt ? "true" : "false";

            title = $"{db.Server}/{db.DB}";

            try
            {
                conn.Close();
                conn.ConnectionString = $"Data Source={db.Server},{db.Port};Initial Catalog={db.DB};User ID={db.User}"
                    + $";Password={db.Pwd};Encrypt={encrypt};Pooling=false;Persist Security Info=True";
                conn.Open();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
        }

        private bool EnableIdentityInsert(string tableName, bool status)
        {
            StringBuilder sb = new StringBuilder()
                .Append("IF EXISTS(SELECT 1 FROM sysobjects WHERE name = '").Append(tableName)
                .Append("' AND OBJECTPROPERTY(id, 'TableHasIdentity') = 1) SET IDENTITY_INSERT ")
                .Append(ProcessTableName(tableName)).Append(status ? " ON" : " OFF");

            return Execute(sb.ToString(), null, out _);
        }

        private string[] ExcludeFields(string[] fields, string[] skipFields)
        {
            if (skipFields == null || skipFields.Length == 0)
                return fields;
            else
            {
                List<string> lst = new List<string>(), skipList = new List<string>();

                foreach (string s in skipFields)
                    skipList.Add(s.ToLower());

                foreach (string s in fields)
                    if (!skipList.Contains(s.ToLower()))
                        lst.Add(s);

                return lst.ToArray();
            }
        }

        public bool ExecScript(Table table, object script, ref uint count)
        {
            try
            {
                if (script is AppendScript dt)
                {
                    SqlBulkCopyOptions option = dt.KeepIdentity ?
                        SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls : SqlBulkCopyOptions.KeepNulls;

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, option, trans)
                    {
                        BatchSize = 2000,
                        DestinationTableName = table.DestName,
                        BulkCopyTimeout = 20
                    })
                    {
                        bulkCopy.WriteToServer(dt.Data);
                        count = (uint)dt.Data.Rows.Count;

                        return true;
                    }
                }
                else if (script is MergeScript ms)
                {
                    if (Execute(ms.PrepareSQL, null, out _))
                        try
                        {
                            SqlBulkCopyOptions option = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls;

                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, option, trans)
                            {
                                BatchSize = 2000,
                                DestinationTableName = ms.TableName,
                                BulkCopyTimeout = 20
                            })
                            {
                                bulkCopy.WriteToServer(ms.Data);

                                if (table.KeepIdentity) EnableIdentityInsert(table.DestName, true);
                                try
                                {
                                    if (Execute(ms.MergeSQL, null, out _))
                                    {
                                        if (!string.IsNullOrEmpty(ms.MergeSQL2))
                                            if (!Execute(ms.MergeSQL2, null, out _))
                                                return false;

                                        count = (uint)ms.Data.Rows.Count;

                                        return true;
                                    }
                                }
                                finally
                                {
                                    if (table.KeepIdentity) EnableIdentityInsert(table.DestName, false);
                                }
                            }
                        }
                        finally
                        {
                            Execute(ms.CleanSQL, null, out _);
                        }
                }
                else if (script is UpdateScript sql)
                {
                    if (Execute(sql.UpdateSQL, null, out count))
                        if (count == 0)
                            return Execute(sql.InsertSQL, null, out count);
                        else
                            return true;

                }

            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
            }

            return false;
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            count = 0;
            try
            {
                SqlCommand cmd = new SqlCommand(sql, conn, trans);

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                count = (uint)cmd.ExecuteNonQuery();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
                Logger.WriteLog(title, sql);

                return false;
            }
        }

        public bool GetFieldNames(string tableName, ref string[] fieldNames)
        {
            IDataWrapper data = null;

            if (Query($"SELECT * FROM {ProcessTableName(tableName)} WHERE 1 = 0", null, ref data))
                try
                {
                    fieldNames = data.GetFieldNames();

                    return true;
                }
                finally
                {
                    data.Close();
                }
            else
                return false;
        }

        private string GetFmtValue(object obj)
        {
            if (obj is DBNull)
                return "NULL";
            else if (obj is string)
                return ProcessString(obj as string);
            else if (obj is DateTime)
                return ProcessString(((DateTime)obj).ToString("yyyy-MM-dd HH:mm:ss.fff"));
            else if (obj is bool)
                return (bool)obj ? "1" : "0";
            else if (obj is byte[])
                return BytesToStr(obj as byte[]);
            else
                return obj.ToString();
        }

        public string GetLastError()
        {
            return errMsg;
        }

        public string GetName()
        {
            return "MSSQL";
        }

        public bool GetTables(IProgress progress, List<TableInfo> lst)
        {
            List<TableFK> fks = new List<TableFK>();
            IDataWrapper data = null;
            int total = 0, position = 0;

            // 获取所有用户表清单
            if (Query("SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name ASC", null, ref data))
                try
                {
                    while (data.Read())
                        fks.Add(new TableFK() { Name = (string)data.GetValue(0), Order = 0 });

                    total = data.ReadCount * 2;
                }
                finally
                {
                    data.Close();
                }

            // 获取每个表的主键字段清单
            foreach (TableFK fk in fks)
            {
                List<string> keys = new List<string>();

                if (Query("SELECT C.name FROM sys.indexes A JOIN sys.index_columns B"
                    + " ON A.object_id = B.object_id AND A.index_id = B.index_id"
                    + " JOIN sys.columns C ON B.object_id = C.object_id AND B.column_id = C.column_id"
                    + $" WHERE A.object_id = OBJECT_ID('{fk.Name}') AND A.is_primary_key = 1"
                    + " ORDER BY B.index_column_id ASC", null, ref data))
                {
                    try
                    {
                        while (data.Read()) keys.Add((string)data.GetValue(0));
                    }
                    finally
                    {
                        data.Close();
                    }

                }

                fk.KeyFields = keys.ToArray();
                progress.OnProgress(total, ++position);
            }

            // 获取每个表的外键指向的表清单
            foreach (TableFK fk in fks)
            {
                if (Query("SELECT C.name FROM sysconstraints A JOIN sysforeignkeys B ON A.constid = B.constid"
                    + " JOIN sysobjects C ON C.type = 'U' AND C.id = B.rkeyid WHERE A.id = OBJECT_ID('"
                    + fk.Name + "')", null, ref data))
                {
                    try
                    {
                        while (data.Read()) fk.FKs.Add((string)data.GetValue(0));
                    }
                    finally
                    {
                        data.Close();
                    }
                }
                progress.OnProgress(total, ++position);
            }

            int order = 100;

            foreach (TableFK fk in fks)
                if (fk.FKs.Count == 0 || (fk.FKs.Count == 1 && fk.FKs[0].Equals(fk.Name)))
                    fk.Order = order;

            order += 100;
            while (order < 10000)
            {
                int left = 0;

                foreach (TableFK fk in fks)
                    if (fk.Order == 0)
                    {
                        left++;

                        bool done = true;

                        foreach (string s in fk.FKs)
                        {
                            bool found = false;

                            foreach (TableFK fk2 in fks)
                            {
                                if (fk2.Name.Equals(s) && fk2.Order > 0)
                                {
                                    found = true;
                                    break;
                                }
                            }

                            if (!found)
                            {
                                done = false;
                                break;
                            }
                        }

                        if (done) fk.Order = order;
                    }

                if (left == 0) break;
                order += 100;
            }

            foreach (TableFK fk in fks)
                lst.Add(new TableInfo() { Name = fk.Name, KeyFields = fk.KeyFields, Order = fk.Order });

            lst.Sort(new TableInfoComparer());

            return true;
        }

        private string[] ModifyFields(string[] fields, string[] skipFields, string prefix)
        {
            if (skipFields == null || skipFields.Length == 0)
                return fields;
            else
            {
                List<string> lst = new List<string>(), skipList = new List<string>();

                foreach (string s in skipFields)
                    skipList.Add(s.ToLower());

                foreach (string s in fields)
                    if (skipList.Contains(s.ToLower()))
                        lst.Add(prefix + s);
                    else
                        lst.Add(s);

                return lst.ToArray();
            }
        }

        private string ProcessFieldName(string fieldName)
        {
            if (string.IsNullOrEmpty(fieldName))
                return "";
            else if (fieldName.StartsWith("["))
                return fieldName;
            else
                return $"[{fieldName}]";
        }

        private string ProcessFieldNames(string[] fields, string prefix = "")
        {
            if (fields == null || fields.Length == 0)
                return "";
            else
            {
                if (prefix == null)
                    prefix = "";
                else if (prefix.Length > 0)
                    prefix += ".";

                StringBuilder sb = new StringBuilder();

                sb.Append(prefix).Append(ProcessFieldName(fields[0]));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(prefix).Append(ProcessFieldName(fields[i]));

                return sb.ToString();
            }
        }

        private string ProcessString(string s)
        {
            if (string.IsNullOrEmpty(s))
                return "''";
            else
            {
                if (s.IndexOf('\'') >= 0)
                    s = s.Replace("'", "''");

                return $"'{s}'";
            }
        }

        private string ProcessTableName(string tableName, WithEnums with = WithEnums.None)
        {
            if (string.IsNullOrEmpty(tableName))
                return "";
            else
            {
                if (!tableName.StartsWith("["))
                    tableName = $"[{tableName}]";

                string s = "";

                if ((WithEnums.NoLock & with) != 0)
                    s += ", NOLOCK";

                if (s.Length > 0)
                    tableName += $" WITH({s.Substring(2)})";

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, ref IDataWrapper reader)
        {
            try
            {
                SqlCommand cmd = new SqlCommand(sql, conn, trans);

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                reader = new IDataReaderWrapper(cmd.ExecuteReader(CommandBehavior.SingleResult));

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
                Logger.WriteLog(title, sql);

                return false;
            }

        }

        public bool QueryCount(string tableName, string whereSQL, WithEnums with, Dictionary<string, object> parms,
            ref ulong count)
        {
            IDataWrapper data = null;
            StringBuilder sb = new StringBuilder()
                .Append("SELECT COUNT(*) AS '_ROW_COUNT_' FROM ").Append(ProcessTableName(tableName, with));

            if (!string.IsNullOrEmpty(whereSQL))
                sb.Append(" WHERE ").Append(whereSQL);

            if (Query(sb.ToString(), parms, ref data))
                try
                {
                    if (data.Read())
                    {
                        object o = data.GetValue(0);

                        if (o.GetType() == typeof(long))
                            count = (ulong)(long)o;
                        else
                            count = (ulong)(int)o;
                    }
                    else
                        count = 0;

                    return true;
                }
                finally
                {
                    data.Close();
                }
            else
                return false;
        }

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms,
            ref IDataWrapper reader)
        {
            // 语法格式形如：仅 2008 或更新版本
            // SELECT <fieldsSQL> FROM (SELECT ROW_NUMBER() OVER (ORDER BY <orderSQL>) AS
            // '_RowNum_', <fieldsSQL> FROM <tableName> {WHERE <whereSQL>}) A
            // WHERE [A].[_RowNum_] BETWEEN <firstRow> AND <toRow>
            // 最后面如果添加排序，则性能将受影响
            string fieldsSQL = ProcessFieldNames(table.SourceFields);
            StringBuilder sb = new StringBuilder()
                .Append("SELECT ").Append(fieldsSQL).Append(" FROM (SELECT ROW_NUMBER() OVER (ORDER BY ")
                .Append(table.OrderSQL).Append(") AS '_RowNum_', ").Append(fieldsSQL).Append(" FROM ")
                .Append(ProcessTableName(table.SourceName, with));

            if (!string.IsNullOrEmpty(table.SourceWhereSQL))
                sb.Append(" WHERE ").Append(table.SourceWhereSQL);

            sb.Append(") A WHERE [A].[_RowNum_] BETWEEN ").Append(fromRow).Append(" AND ").Append(toRow);
            //.Append(" ORDER BY ").Append(orderSQL);

            return Query(sb.ToString(), parms, ref reader);
        }

        public bool QueryParam(string sql, Dictionary<string, object> parms)
        {
            IDataWrapper data = null;

            if (!string.IsNullOrEmpty(sql))
            {
                if (Query(sql, null, ref data))
                    try
                    {
                        if (data.Read())
                        {
                            for (int i = 0; i < data.FieldCount; i++)
                                parms.Add(data.GetFieldName(i), data.GetValue(i));

                            return true;
                        }
                    }
                    finally
                    {
                        data.Close();
                    }

                return false;
            }

            return true;
        }

        public bool RollbackTransaction()
        {
            try
            {
                trans.Rollback();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }
    }
}

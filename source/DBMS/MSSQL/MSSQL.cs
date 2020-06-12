using JHWork.DataMigration.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace JHWork.DataMigration.DBMS.MSSQL
{
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

        public bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            // 有数据
            if (data.Read())
            {
                if (table is MaskingTable)
                    BuildScriptWithMaskSQL(table, data, filter, out script);
                else if (table.WriteMode == WriteModes.Append)
                    BuildScriptWithDataTable(table, data, filter, out script);
                else
                    BuildScriptWithMergeSQL(table, data, filter, out script);

                return true;
            }
            else
            {
                script = null;

                return false;
            }
        }

        private void BuildScriptWithDataTable(Table table, IDataWrapper data, IDataFilter filter,
            out object script)
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

        private void BuildScriptWithMaskSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName);
            string tmpTable = $"{destTable.Substring(1, destTable.Length - 2)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable);

            BuildScriptWithDataTable(table, data, filter, out script);

            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(fields[0]);

            sb.Append($"UPDATE {destTable} SET {field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", {field} = B.{field}");
            }
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append($" FROM {processedTmpTable} B WHERE {destTable}.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND {destTable}.{field} = B.{field}");
            }

            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"SELECT {ProcessFieldNames(table.DestFields)} INTO {processedTmpTable} FROM {destTable} WHERE 1 = 0",
                Data = ((AppendScript)script).Data,
                MergeSQL = sb.ToString(),
                MergeSQL2 = "",
                CleanSQL = $"DROP TABLE {processedTmpTable}"
            };
        }

        private void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName);
            string tmpTable = $"{destTable.Substring(1, destTable.Length - 2)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable);
            string mergeSQL, mergeSQL2;

            BuildScriptWithDataTable(table, data, filter, out script);

            if (Version.Parse(conn.ServerVersion).Major >= 10) // 2008 或更新版本
            {
                StringBuilder sb = new StringBuilder();
                string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
                string field = ProcessFieldName(table.KeyFields[0]);

                sb.Append($"MERGE INTO {destTable} A USING {processedTmpTable} B ON A.{field} = B.{field}");
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    field = ProcessFieldName(table.KeyFields[i]);
                    sb.Append($" AND A.{field} = B.{field}");
                }
                field = ProcessFieldName(fields[0]);
                sb.AppendLine().Append($" WHEN MATCHED THEN UPDATE SET A.{field} = B.{field}");
                for (int i = 1; i < fields.Length; i++)
                {
                    field = ProcessFieldName(fields[i]);
                    sb.Append($", A.{field} = B.{field}");
                }

                fields = ExcludeFields(table.DestFields, table.SkipFields);
                sb.AppendLine().Append($" WHEN NOT MATCHED THEN INSERT ({ProcessFieldNames(fields)}")
                    .Append($") VALUES ({ProcessFieldNames(fields, "B")});"); // 语句以分号结尾

                mergeSQL = sb.ToString();
                mergeSQL2 = "";
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
                string field = ProcessFieldName(fields[0]);

                sb.Append($"UPDATE {destTable} SET {field} = B.{field}");
                for (int i = 1; i < fields.Length; i++)
                {
                    field = ProcessFieldName(fields[i]);
                    sb.Append($", {field} = B.{field}");
                }
                field = ProcessFieldName(table.KeyFields[0]);
                sb.Append($" FROM {processedTmpTable} B WHERE {destTable}.{field} = B.{field}");
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    field = ProcessFieldName(table.KeyFields[i]);
                    sb.Append($" AND {destTable}.{field} = B.{field}");
                }

                mergeSQL = sb.ToString();

                fields = ExcludeFields(table.DestFields, table.SkipFields);
                sb.Length = 0;
                field = ProcessFieldName(table.KeyFields[0]);
                sb.Append($"INSERT INTO {destTable} ({ProcessFieldNames(fields)}) SELECT")
                    .Append($" {ProcessFieldNames(fields, "A")} FROM {processedTmpTable} A LEFT JOIN {destTable} B ON")
                    .Append($"A.{field} = B.{field}");
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    field = ProcessFieldName(table.KeyFields[i]);
                    sb.Append($" AND A.{field} = B.{field}");
                }
                sb.Append($" WHERE B.{field} IS NULL");

                mergeSQL2 = sb.ToString();
            }

            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"SELECT * INTO {processedTmpTable} FROM {destTable} WHERE 1 = 0",
                Data = ((AppendScript)script).Data,
                MergeSQL = mergeSQL,
                MergeSQL2 = mergeSQL2,
                CleanSQL = $"DROP TABLE {processedTmpTable}"
            };
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
                .Append($"IF EXISTS(SELECT 1 FROM sysobjects WHERE name = '{tableName}'")
                .Append(" AND OBJECTPROPERTY(id, 'TableHasIdentity') = 1) SET IDENTITY_INSERT ")
                .Append($"{ProcessTableName(tableName)} ").Append(status ? "ON" : "OFF");

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

        private string[] ExcludeFields(string[] fields, string[] skipFields, string[] skipFields2)
        {
            List<string> skipList = new List<string>();

            if (skipFields != null && skipFields.Length != 0)
                foreach (string s in skipFields)
                    skipList.Add(s.ToLower());

            if (skipFields2 != null && skipFields2.Length != 0)
                foreach (string s in skipFields2)
                    skipList.Add(s.ToLower());

            if (skipList.Count == 0)
                return fields;
            else
            {
                List<string> lst = new List<string>();

                foreach (string s in fields)
                    if (!skipList.Contains(s.ToLower()))
                        lst.Add(s);

                return lst.ToArray();
            }
        }

        public bool ExecScript(Table table, object script, out uint count)
        {
            count = 0;
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
                else if (script is string s)
                    return Execute(s, null, out count);
            }
            catch (Exception ex)
            {
                errMsg = $"{table.DestName}：{ex.Message}";
                Logger.WriteLogExcept(title, ex);
            }

            return false;
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
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
                count = 0;

                return false;
            }
        }

        public bool GetFieldNames(string tableName, out string[] fieldNames)
        {
            if (Query($"SELECT * FROM {ProcessTableName(tableName)} WHERE 1 = 0", null, out IDataWrapper data))
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
            {
                fieldNames = null;
                return false;
            }
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
            int total = 0, position = 0;

            // 获取所有用户表清单
            if (Query("SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name ASC", null, out IDataWrapper data))
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
                    + " ORDER BY B.index_column_id ASC", null, out data))
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
                    + $" JOIN sysobjects C ON C.type = 'U' AND C.id = B.rkeyid WHERE A.id = OBJECT_ID('{fk.Name}')",
                    null, out data))
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
            while (order <= 10000) // 设定一个级别上限：100 级
            {
                int left = 0;
                List<TableFK> lastList = new List<TableFK>();

                // 创建上一轮次的结果清单
                foreach (TableFK fk in fks)
                    if (fk.Order > 0) lastList.Add(fk);

                foreach (TableFK fk in fks)
                    if (fk.Order == 0)
                    {
                        bool done = true;

                        // 检查是否所有外键指向表都在上一轮清单里面
                        foreach (string s in fk.FKs)
                        {
                            bool found = false;

                            foreach (TableFK fk2 in lastList)
                                if (fk2.Name.Equals(s))
                                {
                                    found = true;
                                    break;
                                }

                            if (!found)
                            {
                                done = false;
                                break;
                            }
                        }
                        if (done)
                            fk.Order = order;
                        else
                            left++;
                    }

                if (left == 0) break;
                order += 100;
            }

            foreach (TableFK fk in fks)
                lst.Add(new TableInfo()
                {
                    Name = fk.Name,
                    KeyFields = fk.KeyFields,
                    Order = fk.Order,
                    References = fk.FKs.ToArray()
                });

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

        private string ProcessFieldName(string fieldName, string prefix = "")
        {
            if (string.IsNullOrEmpty(fieldName)) return "";

            if (prefix == null)
                prefix = "";
            else if (prefix.Length > 0)
                prefix += ".";

            if (fieldName.StartsWith("["))
                return prefix + fieldName;
            else
                return prefix + $"[{fieldName}]";
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

        private string ProcessTableName(string tableName, WithEnums with = WithEnums.None, string alias = "")
        {
            if (string.IsNullOrEmpty(tableName))
                return "";
            else
            {
                if (!tableName.StartsWith("["))
                    tableName = $"[{tableName}]";

                if (!string.IsNullOrEmpty(alias))
                    tableName += $" {alias}";

                string s = "";

                if ((WithEnums.NoLock & with) != 0)
                    s += ", NOLOCK";

                if (s.Length > 0)
                    tableName += $" WITH({s.Substring(2)})";

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, out IDataWrapper reader)
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
                reader = null;

                return false;
            }

        }

        public bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count)
        {
            StringBuilder sb = new StringBuilder()
                .Append("SELECT COUNT(*) AS '_ROW_COUNT_' FROM ").Append(ProcessTableName(table.SourceName, with));

            if (!string.IsNullOrEmpty(table.WhereSQL))
            {
                if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                    sb.Append(" WHERE");
                sb.Append($" {table.WhereSQL}");
            }

            count = 0;
            if (Query(sb.ToString(), parms, out IDataWrapper data))
                try
                {
                    if (data.Read())
                        count = ulong.Parse(data.GetValue(0).ToString());

                    return true;
                }
                finally
                {
                    data.Close();
                }
            else
                return false;
        }

        private bool QueryMaxKey(string sql, Dictionary<string, object> parms, out object value)
        {
            if (Query(sql, parms, out IDataWrapper data))
                try
                {
                    if (data.Read())
                    {
                        value = data.GetValue(0);

                        return true;
                    }
                }
                finally
                {
                    data.Close();
                }

            value = null;
            return false;
        }

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms,
            out IDataWrapper reader)
        {
            StringBuilder sb = new StringBuilder();

            // 如果主键字段只有一个：
            // SELECT TOP <toRow - fromRow + 1> <fieldsSQL> FROM <tableName>
            // {WHERE {<keyField> > @LastMaxKey} {AND {<whereSQL>}}} ORDER BY <keyField> ASC
            // 其中
            // @LastMaxKey = SELECT MAX(<keyField>) AS '_MaxKey_' FROM (
            // SELECT TOP <toRow - fromRow + 1> <keyField> FROM <tableName>
            // {WHERE {<keyField> > @LastMaxKey} {AND {<whereSQL>}}} ORDER BY <keyField> ASC
            if (table.KeyFields.Length == 1)
            {
                string tableName = ProcessTableName(table.SourceName, with);
                string keyField = ProcessFieldName(table.KeyFields[0]);
                string keyFieldWithPrefix = ProcessFieldName(table.KeyFields[0], ProcessTableName(table.SourceName));

                // 查询最大键值
                sb.Append($"SELECT MAX({keyField}) AS '_MaxKey_' FROM (")
                    .Append($"SELECT TOP {toRow - fromRow + 1} {keyFieldWithPrefix} FROM {tableName}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append(" ").Append(table.WhereSQL);
                    if (parms.ContainsKey("LastMaxKey"))
                        sb.Append($" AND {keyFieldWithPrefix} > @LastMaxKey");
                }
                else if (parms.ContainsKey("LastMaxKey"))
                    sb.Append($" WHERE {keyFieldWithPrefix} > @LastMaxKey");
                sb.Append($" ORDER BY {keyFieldWithPrefix} ASC) A");

                if (QueryMaxKey(sb.ToString(), parms, out object maxValue))
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, ProcessTableName(table.SourceName));

                    sb.Length = 0;
                    sb.Append($"SELECT TOP {toRow - fromRow + 1} {fieldsSQL} FROM {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" WHERE");
                        sb.Append(" ").Append(table.WhereSQL);
                        if (parms.ContainsKey("LastMaxKey"))
                            sb.Append($" AND {keyFieldWithPrefix} > @LastMaxKey");
                    }
                    else if (parms.ContainsKey("LastMaxKey"))
                        sb.Append($" WHERE {keyFieldWithPrefix} > @LastMaxKey");
                    sb.Append($" ORDER BY {keyFieldWithPrefix} ASC");

                    bool rst = Query(sb.ToString(), parms, out reader);

                    parms["LastMaxKey"] = maxValue;

                    return rst;
                }
                else
                {
                    reader = null;

                    return false;
                }
            }
            else if (Version.Parse(conn.ServerVersion).Major >= 10) // 2008 或更新版本
            {
                // 语法格式形如：
                // SELECT <fieldsSQL> FROM (SELECT ROW_NUMBER() OVER (ORDER BY <orderSQL>)
                // AS '_RowNum_', <fieldsSQL> FROM <tableName>
                // {WHERE <whereSQL>}
                // ) A WHERE A.[_RowNum_] BETWEEN <fromRow> AND <toRow>
                //
                // 如果存在主键，可以优化为：
                // SELECT <B.fieldsSQL> FROM <tableName> B JOIN (SELECT <keyFields> FROM
                // (SELECT <keyFields>, ROW_NUMBER() OVER (ORDER BY <orderSQL>) AS '_RowNum_'
                // FROM <tableName>
                // {WHERE <whereSQL>}
                // ) A WHERE [_RowNum_] BETWEEN <fromRow> AND <toRow>) A ON <B.keyFields> = <A.keyFields>
                if (table.KeyFields.Length > 1)
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, "B");
                    string tableName = ProcessTableName(table.SourceName, with);
                    string tableNameWithB = ProcessTableName(table.SourceName, with, "B");
                    string keyFields = ProcessFieldNames(table.KeyFields);
                    string keyFieldsWithAlias = ProcessFieldNames(table.KeyFields, ProcessTableName(table.SourceName));
                    string keyField = ProcessFieldName(table.KeyFields[0]);

                    sb.Append($"SELECT {fieldsSQL} FROM {tableNameWithB} JOIN (SELECT {keyFields} FROM")
                        .Append($" (SELECT {keyFieldsWithAlias}, ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                        .Append($" AS '_RowNum_' FROM {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" WEHRE ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" WHERE");
                        sb.Append($" {table.WhereSQL}");
                    }
                    sb.Append($") A WHERE A.[_RowNum_] BETWEEN {fromRow} AND {toRow}) A ON B.{keyField} = A.{keyField}");
                    for (int i = 1; i < table.KeyFields.Length; i++)
                    {
                        keyField = ProcessFieldName(table.KeyFields[i]);
                        sb.Append($" AND B.{keyField} = A.{keyField}");
                    }
                }
                else
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields);
                    string fieldsWithAlias = ProcessFieldNames(table.SourceFields, ProcessTableName(table.SourceName));

                    sb.Append($"SELECT {fieldsSQL} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                        .Append($" AS '_RowNum_', {fieldsWithAlias} FROM {ProcessTableName(table.SourceName, with)}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" WHERE");
                        sb.Append($" {table.WhereSQL}");
                    }
                    sb.Append($") A WHERE A.[_RowNum_] BETWEEN {fromRow} AND {toRow}");
                }
            }
            else
            {
                // 此语法要求 whereSQL、orderSQL 包含表名前缀，如：MyTable.KeyField ASC
                // SELECT TOP <toRow - fromRow + 1> <tableName.fieldsSQL> FROM <tableNameWith>
                // LEFT JOIN (SELECT TOP <fromRow - 1> <keyFieldsSQL> FROM <tableNameWith>
                // {WHERE <whereSQL>}
                // ORDER BY <orderSQL>) B ON
                // <tableName.keyFields> = <B.keyFields>
                // WHERE <B.keyFields[0]> IS NULL
                // {AND <whereSQL>}
                string tableName = ProcessTableName(table.SourceName);
                string tableNameWith = ProcessTableName(table.SourceName, with);
                string fieldsSQL = ProcessFieldNames(table.SourceFields, tableName);
                string keyFieldsSQL = ProcessFieldNames(table.KeyFields);
                string keyField = ProcessFieldName(table.KeyFields[0]);

                sb.Append($"SELECT TOP {toRow - fromRow + 1} {fieldsSQL} FROM {tableNameWith}")
                    .Append($" LEFT JOIN (SELECT TOP {fromRow - 1} {keyFieldsSQL} FROM {tableNameWith}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append($" {table.WhereSQL}");
                }
                sb.Append($" ORDER BY {table.OrderSQL}) B ON ").Append($"{tableName}.{keyField} = B.{keyField}");
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    keyField = ProcessFieldName(table.KeyFields[i]);
                    sb.Append($" AND {tableName}.{keyField} = B.{keyField}");
                }
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append($" {table.WhereSQL} AND B.{keyField} IS NULL");
                }
                else
                    sb.Append($" WHERE B.{keyField} IS NULL");
            }

            return Query(sb.ToString(), parms, out reader);
        }

        public bool QueryParam(string sql, Dictionary<string, object> parms)
        {
            if (!string.IsNullOrEmpty(sql))
            {
                if (Query(sql, null, out IDataWrapper data))
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

    /// <summary>
    /// 表外键信息
    /// </summary>
    internal class TableFK : TableInfo
    {
        public List<string> FKs { get; } = new List<string>(); // 外键指向表
    }
}

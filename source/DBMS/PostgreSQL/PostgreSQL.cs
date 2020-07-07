using JHWork.DataMigration.Common;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace JHWork.DataMigration.DBMS.PostgreSQL
{
    /// <summary>
    /// 基于合并算法的更新数据脚本对象
    /// </summary>
    internal class MergeScript
    {
        public string TableName { get; set; }  // 临表名称
        public string PrepareSQL { get; set; } // 准备 SQL（创建临表）
        public DataTable Data { get; set; }    // 数据
        public string UpdateSQL { get; set; }  // 更新 SQL
        public string InsertSQL { get; set; }  // 插入 SQL
        public string CleanSQL { get; set; }   // 清理 SQL
    }

    /// <summary>
    /// PostgreSQL
    /// </summary>
    public class PostgreSQL : DBMSBase, IAssemblyLoader, IDBMSAssistant, IDBMSReader, IDBMSWriter
    {
        private NpgsqlConnection conn;
        private NpgsqlTransaction trans = null;
        private bool isRollback = false;

        public PostgreSQL()
        {
            LogTitle = GetName();
        }

        public bool BeginTransaction()
        {
            try
            {
                trans = conn.BeginTransaction();
                isRollback = false;

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

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

        private void BuildScriptWithDataTable(Table table, IDataWrapper data, IDataFilter filter, out object script)
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

            script = dt;
        }

        private void BuildScriptWithMaskSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = $"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable, table.DestSchema);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(fields[0]);

            sb.Append($"update {destTable} A set {field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", {field} = B.{field}");
            }
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append($" from {processedTmpTable} B where A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" and A.{field} = B.{field}");
            }

            BuildScriptWithDataTable(table, data, filter, out script);

            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"select {ProcessFieldNames(table.DestFields)} into {processedTmpTable} from {destTable} where 1 = 0",
                Data = (DataTable)script,
                UpdateSQL = sb.ToString(),
                InsertSQL = "",
                CleanSQL = $"drop table {processedTmpTable}"
            };
        }

        private void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = $"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable, table.DestSchema);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(fields[0]);

            sb.Append($"update {destTable} A set {field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", {field} = B.{field}");
            }
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append($" from {processedTmpTable} B where A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" and A.{field} = B.{field}");
            }

            string updateSQL = sb.ToString();

            fields = ExcludeFields(table.DestFields, table.SkipFields);
            sb.Length = 0;
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append($"insert into {destTable} ({ProcessFieldNames(fields)}) select {ProcessFieldNames(fields, "A")}")
                .Append($" from {processedTmpTable} A left join {destTable} B on A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" and A.{field} = B.{field}");
            }
            sb.Append($" where B.{field} is null");

            BuildScriptWithDataTable(table, data, filter, out script);

            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"select * into {processedTmpTable} from {destTable} where 1 = 0",
                Data = (DataTable)script,
                UpdateSQL = updateSQL,
                InsertSQL = sb.ToString(),
                CleanSQL = $"drop table {processedTmpTable}"
            };
        }

        public void Close()
        {
            try
            {
                if (isRollback)
                    conn = null; // 为适应异步回滚，此处只做引用数清零
                else
                {
                    conn.Close();
                    conn = null;
                }
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
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }

        public bool Connect(Database db)
        {
            string encrypt = db.Encrypt ? "Require" : "Disable";

            LogTitle = $"{db.Server}:{db.Port}/{db.DB}";
            Schema = db.Schema;
            Timeout = db.Timeout;
            try
            {
                if (conn != null) conn.Close();
                conn = new NpgsqlConnection
                {
                    ConnectionString = $"Server={db.Server};Port={db.Port};Database={db.DB};Userid={db.User}"
                        + $";Password={db.Pwd};Pooling=false;Persist Security Info=True;SslMode={encrypt}"
                };
                conn.Open();

                if (!string.IsNullOrEmpty(Schema)) Execute($"set search_path to {Schema}", null, out _);

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }


        public bool ExecScript(Table table, object script, out uint count)
        {
            return InternalExecScript(table.DestName, table.DestSchema, script, out count);
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            count = 0;
            try
            {
                NpgsqlCommand cmd = new NpgsqlCommand(sql, conn, trans)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                count = (uint)cmd.ExecuteNonQuery();

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);
                Logger.WriteLog(LogTitle, sql);

                return false;
            }
        }

        private string ExtractTableName(string name)
        {
            if (string.IsNullOrEmpty(name)) return "";

            string[] parts = name.Split('.');

            name = parts[parts.Length - 1]; // 最后一段

            return name.Replace("\"", "");
        }

        public bool GetFieldNames(string tableName, string schema, out string[] fieldNames)
        {
            if (Query($"select * from {ProcessTableName(tableName, schema)} where 1 = 0", null, out IDataWrapper data))
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

        public string GetName()
        {
            return "PostgreSQL";
        }

        public DBMSParams GetParams()
        {
            return new DBMSParams()
            {
                CharSet = false,
                Compress = false,
                DefaultPort = "5432"
            };
        }

        protected override string[] GetTableKeys(string table, string schema)
        {
            string sql = "select A.relname from pg_class A"
                + " join pg_constraint B on B.confrelid = A.oid and B.contype = 'f'"
                + $" join pg_class C on C.oid = B.conrelid and C.relname = '{table}'";

            if (!IsEmpty(out string s, schema, Schema))
                sql += $" join pg_namespace D on D.oid = C.relnamespace and D.nspname = '{s}'";

            if (Query(sql + " order by A.relname asc", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTableRefs(string table, string schema)
        {
            string sql = "select C.attname from pg_constraint A"
                + $" join pg_class B on A.conrelid = B.oid and B.relname = '{table}'"
                + " join pg_attribute C on C.attrelid = B.oid and ARRAY_POSITION(A.conkey, C.attnum) > 0";

            if (!IsEmpty(out string s, schema, Schema))
                sql += $" join pg_namespace D on D.oid = B.relnamespace and D.nspname = '{s}'";

            if (Query(sql + " where A.contype = 'p' order by C.attname asc", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTables()
        {
            string sql = "select tablename, schemaname from pg_tables";

            if (!string.IsNullOrEmpty(Schema)) sql += $" where schemaname = '{Schema}'";

            if (Query(sql + " order by schemaname asc, tablename asc", null, out IDataWrapper data))
                try
                {
                    List<string> lst = new List<string>();

                    while (data.Read())
                        lst.Add($"{data.GetValue(1)}.{data.GetValue(0)}");

                    return lst.ToArray();
                }
                finally
                {
                    data.Close();
                }
            else
                return new string[] { };
        }

        private bool InternalExecScript(string table, string schema, object script, out uint count)
        {
            count = 0;
            try
            {
                if (script is DataTable dt)
                {
                    using (NpgsqlBinaryImporter writer = conn.BeginBinaryImport(
                        $"copy {ProcessTableName(table, schema)} from stdin binary"))
                    {
                        foreach (DataRow row in dt.Rows)
                        {
                            object[] values = row.ItemArray;

                            writer.StartRow();
                            for (int i = 0; i < values.Length; i++)
                            {
                                object value = values[i];

                                if (value == DBNull.Value || value == null)
                                    writer.WriteNull();
                                else if (dt.Columns[i].DataType == typeof(bool))
                                    writer.Write((bool)value, NpgsqlDbType.Bit);
                                else
                                    writer.Write(value);
                            }
                        }
                    }

                    count = (uint)dt.Rows.Count;

                    return true;
                }
                else if (script is MergeScript ms)
                {
                    if (Execute(ms.PrepareSQL, null, out _))
                        try
                        {
                            if (InternalExecScript(ms.TableName, schema, ms.Data, out count))
                                if (Execute(ms.UpdateSQL, null, out _))
                                    if (string.IsNullOrEmpty(ms.InsertSQL) || Execute(ms.InsertSQL, null, out _))
                                        return true;
                        }
                        finally
                        {
                            Execute(ms.CleanSQL, null, out _);
                        }
                }
                else if (script is UpdateScript us)
                {
                    if (Execute(us.UpdateSQL, null, out count))
                        if (count == 0)
                            return Execute(us.InsertSQL, null, out count);
                        else
                            return true;
                }
                else if (script is string sql)
                    return Execute(sql, null, out count);
            }
            catch (Exception ex)
            {
                LastError = $"{table}：{ex.Message}";
                Logger.WriteLogExcept(LogTitle, ex);
            }

            return false;
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

            return $"{prefix}\"{fieldName}\"";
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

        private string ProcessTableName(string tableName, string schema)
        {
            if (string.IsNullOrEmpty(tableName))
                return "";
            else
            {
                if (!tableName.StartsWith("\""))
                    tableName = $"\"{tableName}\"";

                if (!IsEmpty(out string s, schema, Schema))
                    if (!s.StartsWith("\""))
                        tableName = $"\"{s}\".{tableName}";
                    else
                        tableName = $"{s}.{tableName}";

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            try
            {
                NpgsqlCommand cmd = new NpgsqlCommand(sql, conn, trans)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                reader = new IDataReaderWrapper(cmd.ExecuteReader(CommandBehavior.SingleResult));

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);
                Logger.WriteLog(LogTitle, sql);
                reader = null;

                return false;
            }

        }
        public bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count)
        {
            StringBuilder sb = new StringBuilder()
                .Append("select count(*) as _row_count_ from ").Append(ProcessTableName(table.SourceName, table.SourceSchema));

            if (!string.IsNullOrEmpty(table.WhereSQL))
            {
                if (table.WhereSQL.IndexOf(" where ", StringComparison.OrdinalIgnoreCase) < 0)
                    sb.Append(" where");
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
            string tableName = ProcessTableName(table.SourceName, table.SourceSchema);

            // 语法格式形如：
            // select <fieldsSQL> from <tableName> {where <whereSQL>}
            // {order by <orderSQL>} limit <toRow - fromRow + 1> offset <fromRow - 1>
            //
            // 如果存在主键，可以优化为：
            // select <A.fieldsSQL> from <tableName> A join (select <keyFields> from <tableName> {where <whereSQL>}
            // {order by <orderSQL>} limit <toRow - fromRow + 1> offset <fromRow - 1>) B on <A.keyFields> = <B.keyFields>
            //
            // 如果主键字段只有一个，可以进一步优化为：
            // select <fieldsSQL> from <tableName> {where {<keyField> > @LastMaxKey} {and {<whereSQL>}}
            // order by <keyField> asc limit <toRow - fromRow + 1>
            // 其中
            // @LastMaxKey = select max(<keyField>) as "_MaxKey_" from (select <keyField> from <tableName>
            // {where {<keyField> > @LastMaxKey} {and {<whereSQL>}}} order by <keyField> asc
            // limit <toRow - fromRow + 1>) A
            if (table.KeyFields.Length == 1)
            {
                string keyField = ProcessFieldName(table.KeyFields[0]);
                string keyFieldWithPrefix = ProcessFieldName(table.KeyFields[0], tableName);

                // 查询最大键值
                sb.Append($"select max({keyField}) as \"_MaxKey_\" from (select {keyFieldWithPrefix} from {tableName}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" where ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" where");
                    sb.Append(" ").Append(table.WhereSQL);
                    if (parms.ContainsKey("LastMaxKey"))
                        sb.Append($" and {keyFieldWithPrefix} > @LastMaxKey");
                }
                else if (parms.ContainsKey("LastMaxKey"))
                    sb.Append($" where {keyFieldWithPrefix} > @LastMaxKey");
                sb.Append($" order by {keyField} limit {toRow - fromRow + 1}) A");

                if (QueryMaxKey(sb.ToString(), parms, out object maxValue))
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, tableName);

                    sb.Length = 0;
                    sb.Append($"select {fieldsSQL} from {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" where ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" where");
                        sb.Append(" ").Append(table.WhereSQL);
                        if (parms.ContainsKey("LastMaxKey"))
                            sb.Append($" and {keyFieldWithPrefix} > @LastMaxKey");
                    }
                    else if (parms.ContainsKey("LastMaxKey"))
                        sb.Append($" where {keyFieldWithPrefix} > @LastMaxKey");
                    sb.Append($" order by {keyField} limit {toRow - fromRow + 1}");

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
            else
            {
                if (table.KeyFields.Length > 0)
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, "A");
                    string keyField = ProcessFieldName(table.KeyFields[0]);
                    string keyFields = ProcessFieldNames(table.KeyFields, tableName);

                    sb.Append($"select {fieldsSQL} from {tableName} A join (select {keyFields} from {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" where ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" where");
                        sb.Append($" {table.WhereSQL}");
                    }
                    if (!string.IsNullOrEmpty(table.OrderSQL))
                        sb.Append(" order by ").Append(table.OrderSQL);
                    sb.Append($" limit {toRow - fromRow + 1} offset {fromRow - 1}) B on A.{keyField} = B.{keyField}");
                    for (int i = 1; i < table.KeyFields.Length; i++)
                    {
                        keyField = ProcessFieldName(table.KeyFields[i]);
                        sb.Append($" and A.{keyField} = B.{keyField}");
                    }
                }
                else
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, tableName);

                    sb.Append($"select {fieldsSQL} from {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" where ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" where");
                        sb.Append($" {table.WhereSQL}");
                    }
                    if (!string.IsNullOrEmpty(table.OrderSQL))
                        sb.Append(" order by ").Append(table.OrderSQL);
                    sb.Append($" limit {toRow - fromRow + 1} offset {fromRow - 1}");
                }

                return Query(sb.ToString(), parms, out reader);
            }
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
                trans.RollbackAsync(); // 异步回滚
                isRollback = true;

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }
    }

    /// <summary>
    /// 基于更新和插入 SQL 的更新数据脚本对象
    /// </summary>
    internal class UpdateScript
    {
        public string UpdateSQL { get; set; } // 更新 SQL
        public string InsertSQL { get; set; } // 插入 SQL
    }
}

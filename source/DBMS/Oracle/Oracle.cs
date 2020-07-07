using JHWork.DataMigration.Common;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace JHWork.DataMigration.DBMS.Oracle
{
    /// <summary>
    /// 基于合并算法的更新数据脚本对象
    /// </summary>
    internal class MergeScript
    {
        public string PrepareSQL { get; set; } // 准备 SQL（创建临表）
        public string InsertSQL { get; set; }  // 数据
        public string MergeSQL { get; set; }   // 更新 SQL
        public string CleanSQL { get; set; }   // 清理 SQL
        public string CleanSQL2 { get; set; }  // 清理 SQL 2
    }

    /// <summary>
    /// Oracle
    /// </summary>
    public class Oracle : DBMSBase, IAssemblyLoader, IDBMSAssistant, IDBMSReader, IDBMSWriter
    {
        private readonly OracleConnection conn = new OracleConnection();
        private OracleTransaction trans = null;

        public Oracle()
        {
            LogTitle = GetName();
        }

        public bool BeginTransaction()
        {
            try
            {
                trans = conn.BeginTransaction();

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
            if (data.Read())
            {
                if (table is MaskingTable)
                    BuildScriptWithMaskSQL(table, data, filter, out script);
                else if (table.WriteMode == WriteModes.Append)
                    BuildScriptWithInsertSQL(table, table.DestName, data, filter, out script);
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

        private void BuildScriptWithInsertSQL(Table table, string tableName, IDataWrapper data, IDataFilter filter,
            out object script)
        {
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);
            string fieldsSQL = ProcessFieldNames(fields);
            string tableSQL = ProcessTableName(tableName, table.DestSchema);

            data.MapFields(fields);

            sb.Append("INSERT ALL").AppendLine().Append($"INTO {tableSQL} ({fieldsSQL}) VALUES (")
                .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                r++;
                sb.AppendLine().Append($"INTO {tableSQL} ({fieldsSQL}) VALUES (")
                    .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
                sb.Append(")");
            }
            sb.AppendLine().Append("SELECT * FROM DUAL");

            script = sb.ToString();
        }

        private void BuildScriptWithMaskSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = ProcessTableName($"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}", table.DestSchema);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(table.KeyFields[0]);

            sb.Append($"MERGE INTO {destTable} A USING {tmpTable} B ON (A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND A.{field} = B.{field}");
            }
            sb.Append(")");

            field = ProcessFieldName(fields[0]);
            sb.AppendLine().Append($" WHEN MATCHED THEN UPDATE SET A.{field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", A.{field} = B.{field}");
            }
            sb.Append(";"); // 语句以分号结尾

            BuildScriptWithInsertSQL(table, tmpTable, data, filter, out script);
            script = new MergeScript()
            {
                PrepareSQL = $"CREATE GLOBAL TEMPORARY TABLE {tmpTable} ON COMMIT PRESERVE ROWS AS"
                    + $" SELECT {ProcessFieldNames(table.DestFields)} FROM {destTable} WHERE 1 = 0",
                InsertSQL = script as string,
                MergeSQL = sb.ToString(),
                CleanSQL = $"TRUNCATE TABLE {tmpTable}",
                CleanSQL2 = $"DROP TABLE {tmpTable}"
            };
        }

        private void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = ProcessTableName($"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}", table.DestSchema);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(table.KeyFields[0]);

            sb.Append($"MERGE INTO {destTable} A USING {tmpTable} B ON (A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND A.{field} = B.{field}");
            }
            sb.Append(")");
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

            BuildScriptWithInsertSQL(table, tmpTable, data, filter, out script);
            script = new MergeScript()
            {
                PrepareSQL = $"CREATE GLOBAL TEMPORARY TABLE {tmpTable} ON COMMIT PRESERVE ROWS AS SELECT * FROM {destTable} WHERE 1 = 0",
                InsertSQL = script as string,
                MergeSQL = sb.ToString(),
                CleanSQL = $"TRUNCATE TABLE {tmpTable}",
                CleanSQL2 = $"DROP TABLE {tmpTable}"
            };
        }

        private string BytesToStr(byte[] bytes)
        {
            if (bytes == null) return "NULL";

            StringBuilder sb = new StringBuilder("TO_BLOB('", bytes.Length * 2 + 16); // 冗余几个字符，确保不触发内存扩展

            foreach (byte b in bytes)
                sb.Append(b.ToString("X2"));

            return sb.Append("')").ToString();
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
            LogTitle = $"{db.Server}:{db.Port}/{db.DB}";
            Schema = db.Schema;
            Timeout = db.Timeout;
            try
            {
                conn.Close();
                conn.ConnectionString = $"Data Source={db.Server}:{db.Port}/{db.DB};User ID={db.User}"
                    + $";Password={db.Pwd};Pooling=false;Persist Security Info=True";
                conn.Open();

                if (!string.IsNullOrEmpty(Schema))
                    Execute($"ALTER SESSION SET CURRENT_SCHEMA = {Schema}", null, out _);

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
            count = 0;
            if (script is string sql)
                return Execute(sql, null, out count);
            else if (script is MergeScript ms)
            {
                if (Execute(ms.PrepareSQL, null, out _))
                    try
                    {
                        if (Execute(ms.InsertSQL, null, out count))
                            if (Execute(ms.MergeSQL, null, out _))
                                return true;
                    }
                    finally
                    {
                        Execute(ms.CleanSQL, null, out _);
                        Execute(ms.CleanSQL2, null, out _);
                    }
            }

            return false;
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            try
            {
                OracleCommand cmd = new OracleCommand(sql, conn)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text,
                    Transaction = trans
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.Add(key, parms[key]);

                count = (uint)cmd.ExecuteNonQuery();

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);
                Logger.WriteLog(LogTitle, sql);
                count = 0;

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
            if (Query($"SELECT * FROM {ProcessTableName(tableName, schema)} WHERE 1 = 0", null, out IDataWrapper data))
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

        private string GetFmtValue(object obj)
        {
            if (obj is DBNull)
                return "NULL";
            else if (obj is string)
                return ProcessString(obj as string);
            else if (obj is DateTime dt)
                return ProcessString(dt.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            else if (obj is bool b)
                return b ? "1" : "0";
            else if (obj is byte[])
                return BytesToStr(obj as byte[]);
            else
                return obj.ToString();
        }

        public string GetName()
        {
            return "Oracle";
        }

        public DBMSParams GetParams()
        {
            return new DBMSParams()
            {
                CharSet = false,
                Encrypt = false,
                Compress = false,
                DefaultPort = "1521"
            };
        }

        protected override string[] GetTableKeys(string table, string schema)
        {
            string sql = "SELECT A.COLUMN_NAME FROM ALL_CONS_COLUMNS A JOIN ALL_CONSTRAINTS B"
                + " ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND B.CONSTRAINT_TYPE = 'P'"
                + $" AND B.TABLE_NAME = '{table}'";

            if (!IsEmpty(out string s, schema, Schema)) sql += $" AND B.OWNER = '{s}'";

            if (Query(sql + " ORDER BY A.COLUMN_NAME ASC", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTableRefs(string table, string schema)
        {
            string sql = "SELECT A.TABLE_NAME FROM ALL_CONSTRAINTS A JOIN ALL_CONSTRAINTS B"
                + " ON A.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME AND B.CONSTRAINT_TYPE = 'R'"
                + $" AND B.TABLE_NAME = '{table}'";

            if (!IsEmpty(out string s, schema, Schema)) sql += $" AND B.OWNER = '{s}'";

            if (Query(sql, null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTables()
        {
            string sql = "SELECT TABLE_NAME, OWNER FROM ALL_ALL_TABLES";

            if (!string.IsNullOrEmpty(Schema)) sql += $" WHERE OWNER = '{Schema}'";

            if (Query(sql + " ORDER BY OWNER ASC, TABLE_NAME ASC", null, out IDataWrapper data))
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

        private string ProcessFieldName(string fieldName, string prefix = "")
        {
            if (string.IsNullOrEmpty(fieldName)) return "";

            if (prefix == null)
                prefix = "";
            else if (prefix.Length > 0)
                prefix += ".";

            if (fieldName.StartsWith("\""))
                return prefix + fieldName;
            else
                return prefix + $"\"{fieldName}\"";
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

        private string ProcessTableName(string tableName, string schema, string alias = "")
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

                if (!string.IsNullOrEmpty(alias))
                    tableName += " " + alias;

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            try
            {
                OracleCommand cmd = new OracleCommand(sql, conn)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text,
                    Transaction = trans
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.Add(key, parms[key]);

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
                .Append("SELECT COUNT(*) AS \"_ROW_COUNT_\" FROM ").Append(ProcessTableName(table.SourceName, table.SourceSchema));

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

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            StringBuilder sb = new StringBuilder();

            // 如果主键字段只有一个：
            // SELECT <fieldsSQL> FROM <tableName> WHERE ROWNUM <= <toRow - fromRow + 1>
            // { AND <keyField> > @LastMaxKey}{ AND {<whereSQL>}} ORDER BY <keyField> ASC
            // 其中
            // @LastMaxKey = SELECT MAX(<keyField>) AS "_MaxKey_" FROM (
            // SELECT <keyField> FROM <tableName> WHERE ROWNUM <= <toRow - fromRow + 1>
            // { AND <keyField> > @LastMaxKey}{ AND {<whereSQL>}} ORDER BY <keyField> ASC
            if (table.KeyFields.Length == 1)
            {
                string tableName = ProcessTableName(table.SourceName, table.SourceSchema);
                string keyField = ProcessFieldName(table.KeyFields[0]);
                string keyFieldWithPrefix = ProcessFieldName(table.KeyFields[0], ProcessTableName(table.SourceName, table.SourceSchema));

                // 查询最大键值
                sb.Append($"SELECT MAX({keyField}) AS \"_MaxKey_\" FROM (")
                    .Append($"SELECT {keyFieldWithPrefix} FROM {tableName}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append(" ").Append(table.WhereSQL);
                    if (parms.ContainsKey("LastMaxKey"))
                        sb.Append($" AND {keyFieldWithPrefix} > @LastMaxKey");
                    sb.Append(" AND");
                }
                else if (parms.ContainsKey("LastMaxKey"))
                    sb.Append($" WHERE {keyFieldWithPrefix} > @LastMaxKey AND");
                else
                    sb.Append(" WHERE");
                sb.Append($" ROWNUM <= {toRow - fromRow + 1} ORDER BY {keyFieldWithPrefix} ASC) A");

                if (QueryMaxKey(sb.ToString(), parms, out object maxValue))
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, ProcessTableName(table.SourceName, table.SourceSchema));

                    sb.Length = 0;
                    sb.Append($"SELECT {fieldsSQL} FROM {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" WHERE");
                        sb.Append(" ").Append(table.WhereSQL);
                        if (parms.ContainsKey("LastMaxKey"))
                            sb.Append($" AND {keyFieldWithPrefix} > @LastMaxKey");
                        sb.Append(" AND");
                    }
                    else if (parms.ContainsKey("LastMaxKey"))
                        sb.Append($" WHERE {keyFieldWithPrefix} > @LastMaxKey AND");
                    else
                        sb.Append(" WHERE");
                    sb.Append($" ROWNUM <= {toRow - fromRow + 1} ORDER BY {keyFieldWithPrefix} ASC");

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
            // 如果存在复合主键：
            // SELECT <B.fieldsSQL> FROM <tableName> B JOIN (SELECT <keyFields> FROM
            // (SELECT <keyFields>, ROW_NUMBER() OVER (ORDER BY <orderSQL>) AS "_RowNum_"
            // FROM <tableName>
            // {WHERE <whereSQL>}
            // ) A WHERE "_RowNum_" BETWEEN <fromRow> AND <toRow>) A ON <B.keyFields> = <A.keyFields>
            else if (table.KeyFields.Length > 1)
            {
                string fieldsSQL = ProcessFieldNames(table.SourceFields, "B");
                string tableName = ProcessTableName(table.SourceName, table.SourceSchema);
                string tableNameWithB = ProcessTableName(table.SourceName, table.SourceSchema, "B");
                string keyFields = ProcessFieldNames(table.KeyFields);
                string keyFieldsWithAlias = ProcessFieldNames(table.KeyFields, ProcessTableName(table.SourceName, table.SourceSchema));
                string keyField = ProcessFieldName(table.KeyFields[0]);

                sb.Append($"SELECT {fieldsSQL} FROM {tableNameWithB} JOIN (SELECT {keyFields} FROM")
                    .Append($" (SELECT {keyFieldsWithAlias}, ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                    .Append($" AS \"_RowNum_\" FROM {tableName}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WEHRE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append($" {table.WhereSQL}");
                }
                sb.Append($") A WHERE A.\"_RowNum_\" BETWEEN {fromRow} AND {toRow}) A ON B.{keyField} = A.{keyField}");
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    keyField = ProcessFieldName(table.KeyFields[i]);
                    sb.Append($" AND B.{keyField} = A.{keyField}");
                }

                return Query(sb.ToString(), parms, out reader);
            }
            else
            {
                // 语法格式形如：
                // SELECT <fieldsSQL> FROM (SELECT ROW_NUMBER() OVER (ORDER BY <orderSQL>)
                // AS "_RowNum_", <fieldsSQL> FROM <tableName>
                // {WHERE <whereSQL>}
                // ) A WHERE A."_RowNum_" BETWEEN <fromRow> AND <toRow>
                string fieldsSQL = ProcessFieldNames(table.SourceFields);
                string fieldsWithAlias = ProcessFieldNames(table.SourceFields, ProcessTableName(table.SourceName, table.SourceSchema));

                sb.Append($"SELECT {fieldsSQL} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                    .Append($" AS \"_RowNum_\", {fieldsWithAlias} FROM {ProcessTableName(table.SourceName, table.SourceSchema)}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append($" {table.WhereSQL}");
                }
                sb.Append($") A WHERE A.\"_RowNum_\" BETWEEN {fromRow} AND {toRow}");

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
                trans.Rollback();

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
}

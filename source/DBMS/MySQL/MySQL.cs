using JHWork.DataMigration.Common;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace JHWork.DataMigration.DBMS.MySQL
{
    /// <summary>
    /// CSV 数据脚本对象
    /// </summary>
    internal class CSVScript
    {
        public string CSVFile { get; set; }  // CSV 文件名
        public string[] Fields { get; set; } // 字段清单
        public uint Count { get; set; }      // 记录数

        ~CSVScript()
        {
            // 清理文件
            if (File.Exists(CSVFile))
                try
                {
                    File.Delete(CSVFile);
                }
                catch (Exception ex)
                {
                    Logger.WriteLogExcept(CSVFile, ex);
                }
        }
    }

    /// <summary>
    /// 文件流写入类，提供类似 StringBuilder 用法的快速文件流写入功能。
    /// </summary>
    internal class FileStreamWriter
    {
        private readonly FileStream fs;
        private readonly StreamWriter writer;

        public FileStreamWriter(string file, Encoding encoding)
        {
            fs = new FileStream(file, FileMode.Create);
            writer = new StreamWriter(fs, encoding, 64 * 1024) // 64K 缓冲区
            {
                AutoFlush = false
            };
        }

        /// <summary>
        /// 写入指定字符
        /// </summary>
        /// <param name="c">字符</param>
        /// <returns>本实例</returns>
        public FileStreamWriter Append(char c)
        {
            writer.Write(c);

            return this;
        }

        /// <summary>
        /// 写入指定内容
        /// </summary>
        /// <param name="s">内容</param>
        /// <returns>本实例</returns>
        public FileStreamWriter Append(string s)
        {
            writer.Write(s);

            return this;
        }

        /// <summary>
        /// 写入换行符
        /// </summary>
        /// <returns>本实例</returns>
        public FileStreamWriter AppendLine()
        {
            writer.WriteLine();

            return this;
        }

        /// <summary>
        /// 刷新并关闭
        /// </summary>
        public void Close()
        {
            writer.Flush();
            writer.Close();
            fs.Close();
        }
    }

    /// <summary>
    /// 基于合并算法的更新数据脚本对象
    /// </summary>
    internal class MergeScript
    {
        public string TableName { get; set; }  // 临表名称
        public string PrepareSQL { get; set; } // 准备 SQL（创建临表）
        public object Data { get; set; }       // 数据
        public string UpdateSQL { get; set; }  // 更新 SQL
        public string InsertSQL { get; set; }  // 插入 SQL
        public string CleanSQL { get; set; }   // 清理 SQL
    }

    /// <summary>
    /// MySQL
    /// </summary>
    public class MySQL : DBMSBase, IAssemblyLoader, IDBMSAssistant, IDBMSReader, IDBMSWriter
    {
        private MySqlConnection conn = null;
        private MySqlTransaction trans = null;
        private bool isRollback = false;
        private bool isSupportCSV = false;

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
            if (data.Read())
            {
                if (table is MaskingTable)
                    BuildScriptWithMaskSQL(table, data, filter, out script);
                else if (table.WriteMode == WriteModes.Append)
                    if (isSupportCSV)
                        BuildScriptWithCSV(table, data, filter, out script);
                    else
                        BuildScriptWithInsertSQL(table, table.DestName, data, filter, out script);
                else if (!isSupportCSV)
                    BuildScriptWithReplaceSQL(table, table.DestName, data, filter, out script);
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

        private void BuildScriptWithCSV(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string file = Path.GetTempFileName();
            FileStreamWriter fc = new FileStreamWriter(file, new UTF8Encoding(false));
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);
            uint r = 1;

            data.MapFields(fields);
            try
            {
                // 每行最后增加一个逗号分隔符，以免最后一个字段是字符串时解析有误
                fc.Append(GetCSVValue(filter.GetValue(data, 0, fields[0]))).Append(',');
                for (int i = 1; i < fields.Length; i++)
                    fc.Append(GetCSVValue(filter.GetValue(data, i, fields[i]))).Append(',');

                while (r < table.PageSize && data.Read())
                {
                    fc.AppendLine().Append(GetCSVValue(filter.GetValue(data, 0, fields[0]))).Append(',');
                    for (int i = 1; i < fields.Length; i++)
                        fc.Append(GetCSVValue(filter.GetValue(data, i, fields[i]))).Append(',');

                    r++;
                }
            }
            finally
            {
                fc.Close();
            }

            for (int i = 0; i < fields.Length; i++)
                fields[i] = ProcessFieldName(fields[i]);

            script = new CSVScript() { CSVFile = file, Fields = fields, Count = r };
        }

        private void BuildScriptWithMaskSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = $"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable, table.DestSchema);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(table.KeyFields[0]);

            sb.Append($"UPDATE {destTable} A JOIN {processedTmpTable} B ON A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND A.{field} = B.{field}");
            }
            field = ProcessFieldName(fields[0]);
            sb.Append($" SET A.{field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", A.{field} = B.{field}");
            }

            if (isSupportCSV)
                BuildScriptWithCSV(table, data, filter, out script);
            else
                BuildScriptWithInsertSQL(table, tmpTable, data, filter, out script);

            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"CREATE TEMPORARY TABLE {processedTmpTable} (SELECT {ProcessFieldNames(table.DestFields)} FROM {destTable} WHERE 1 = 0)",
                Data = script,
                UpdateSQL = sb.ToString(),
                InsertSQL = "",
                CleanSQL = $"DROP TEMPORARY TABLE IF EXISTS {processedTmpTable}"
            };
        }

        private void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName,table.DestSchema);
            string tmpTable = $"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable, table.DestSchema);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(table.KeyFields[0]);

            sb.Append($"UPDATE {destTable} A JOIN {processedTmpTable} B ON A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND A.{field} = B.{field}");
            }
            field = ProcessFieldName(fields[0]);
            sb.Append($" SET A.{field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", A.{field} = B.{field}");
            }

            string updateSQL = sb.ToString();

            fields = ExcludeFields(table.DestFields, table.SkipFields);
            sb.Length = 0;
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append($"INSERT INTO {destTable} ({ProcessFieldNames(fields)}) SELECT {ProcessFieldNames(fields, "A")}")
                .Append($" FROM {processedTmpTable} A LEFT JOIN {destTable} B ON A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND A.{field} = B.{field}");
            }
            sb.Append($" WHERE B.{field} IS NULL");

            if (isSupportCSV)
                BuildScriptWithCSV(table, data, filter, out script);
            else
                BuildScriptWithInsertSQL(table, tmpTable, data, filter, out script);

            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"CREATE TEMPORARY TABLE {processedTmpTable} LIKE {destTable}",
                Data = script,
                UpdateSQL = updateSQL,
                InsertSQL = sb.ToString(),
                CleanSQL = $"DROP TEMPORARY TABLE IF EXISTS {processedTmpTable}"
            };
        }

        private void BuildScriptWithInsertSQL(Table table, string tableName, IDataWrapper data, IDataFilter filter,
            out object script)
        {
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);

            data.MapFields(fields);

            sb.Append($"INSERT INTO {ProcessTableName(tableName, table.DestSchema)} ({ProcessFieldNames(fields)})")
                .AppendLine().Append("VALUES").AppendLine()
                .Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                r++;
                sb.Append(",").AppendLine().Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
                sb.Append(")");
            }

            script = sb.ToString();
        }

        private void BuildScriptWithReplaceSQL(Table table, string tableName, IDataWrapper data, IDataFilter filter,
            out object script)
        {
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);

            data.MapFields(fields);

            sb.Append($"REPLACE INTO {ProcessTableName(tableName, table.DestSchema)} ({ProcessFieldNames(fields)})")
                .AppendLine().Append("VALUES").AppendLine()
                .Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                r++;
                sb.Append(",").AppendLine().Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
                sb.Append(")");
            }

            script = sb.ToString();
        }

        private string BytesToStr(byte[] bytes)
        {
            if (bytes == null) return "NULL";

            StringBuilder sb = new StringBuilder("0x", bytes.Length * 2 + 4); // 冗余两个字符，确保不触发内存扩展

            foreach (byte b in bytes)
                sb.Append(b.ToString("X2"));

            return sb.ToString();
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
            string compress = db.Compress ? "true" : "false";
            string encrypt = db.Encrypt ? "Preferred" : "None";

            LogTitle = $"{db.Server}:{db.Port}/{db.DB}";
            Schema = db.DB;
            Timeout = db.Timeout;
            try
            {
                if (conn != null) conn.Close();
                conn = new MySqlConnection
                {
                    ConnectionString = $"Data Source={db.Server};Port={db.Port};Initial Catalog={db.DB}"
                        + $";User ID={db.User};Password={db.Pwd};CharSet={db.CharSet};Pooling=false"
                        + $";Persist Security Info=True;allowLoadLocalInfile=true;SSL Mode={encrypt};Compress={compress}"
                };
                conn.Open();
                isSupportCSV = GetSupportCSV();

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
            return InternalExecScript(table.DestName, script, out count);
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            try
            {
                MySqlCommand cmd = new MySqlCommand(sql, conn, trans)
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
                count = 0;

                return false;
            }
        }

        private string ExtractTableName(string name)
        {
            if (string.IsNullOrEmpty(name)) return "";

            string[] parts = name.Split('.');

            name = parts[parts.Length - 1]; // 最后一段

            return name.Replace("`", "");
        }

        private string GetCSVValue(object obj)
        {
            if (obj is DBNull)
                return "\\N";
            else if (obj is string)
            {
                string s = obj as string;

                if (s.IndexOf('\\') >= 0) s = s.Replace("\\", "\\\\");
                if (s.IndexOf('\"') >= 0) s = s.Replace("\"", "\"\"");

                return $"\"{s}\"";
            }
            else if (obj is DateTime dt)
                return dt.ToString("yyyy-MM-dd HH:mm:ss.fff");
            else if (obj is bool b)
                return b ? "1" : "0";
            else if (obj is byte[])
                return BytesToStr(obj as byte[]);
            else
                return obj.ToString();
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
            return "MySQL";
        }

        public DBMSParams GetParams()
        {
            return new DBMSParams()
            {
                Schema = false,
                DefaultCharSet = "utf8mb4",
                DefaultPort = "3306"
            };
        }

        private bool GetSupportCSV()
        {
            if (Query("SHOW VARIABLES LIKE \"local_infile\"", null, out IDataWrapper data))
                try
                {
                    if (data.Read() && "ON".Equals(((string)data.GetValue(1)).ToUpper()))
                        return true;
                }
                finally
                {
                    data.Close();
                }

            return false;
        }

        protected override string[] GetTableKeys(string table, string schema)
        {
            IsEmpty(out string s, schema, Schema);

            if (Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
                + $" WHERE TABLE_NAME = \"{table}\" AND CONSTRAINT_NAME = \"PRIMARY\""
                + $" AND TABLE_SCHEMA = \"{s}\" ORDER BY ORDINAL_POSITION ASC", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTableRefs(string table, string schema)
        {
            IsEmpty(out string s, schema, Schema);

            if (Query("SELECT REFERENCED_TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
                + $" WHERE TABLE_SCHEMA = \"{s}\" AND CONSTRAINT_NAME <> \"PRIMARY\""
                + $" AND TABLE_NAME = \"{table}\" AND REFERENCED_TABLE_NAME IS NOT NULL", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTables()
        {
            if (Query("SHOW TABLES", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        private bool InternalExecScript(string table, object script, out uint count)
        {
            count = 0;
            if (script is string sql)
                return Execute(sql, null, out count);
            else if (script is CSVScript obj)
            {
                try
                {
                    MySqlBulkLoader bulk = new MySqlBulkLoader(conn)
                    {
                        Local = true,
                        FieldTerminator = ",",
                        FieldQuotationCharacter = '"',
                        EscapeCharacter = '\\',
                        LineTerminator = "\n",
                        FileName = obj.CSVFile,
                        NumberOfLinesToSkip = 0,
                        TableName = table,
                        CharacterSet = "utf8"
                    };

                    bulk.Columns.AddRange(obj.Fields);

                    count = (uint)bulk.Load();
                    if (count != obj.Count) LastError = $"{table}：写入记录数错误！应写入 {obj.Count}，实际写入 {count}。";

                    return count == obj.Count;
                }
                catch (Exception ex)
                {
                    LastError = $"{table}：{ex.Message}";
                    Logger.WriteLogExcept(LogTitle, ex);
                }
            }
            else if (script is MergeScript ms)
            {
                if (Execute(ms.PrepareSQL, null, out _))
                    try
                    {
                        if (InternalExecScript(ms.TableName, ms.Data, out count))
                            if (Execute(ms.UpdateSQL, null, out _))
                                if (string.IsNullOrEmpty(ms.InsertSQL) || Execute(ms.InsertSQL, null, out _))
                                    return true;
                    }
                    finally
                    {
                        Execute(ms.CleanSQL, null, out _);
                    }
            }

            return false;
        }

        private string ProcessFieldName(string fieldName, string prefix = "")
        {
            if (string.IsNullOrEmpty(fieldName)) return "";

            if (prefix == null)
                prefix = "";
            else if (prefix.Length > 0)
                prefix += ".";

            if (fieldName.StartsWith("`"))
                return prefix + fieldName;
            else
                return $"{prefix}`{fieldName}`";
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
                return "\"\"";
            else
            {
                if (s.IndexOf('\\') >= 0)
                    s = s.Replace("\\", "\\\\");

                if (s.IndexOf('\"') >= 0)
                    s = s.Replace("\"", "\\\"");

                return $"\"{s}\"";
            }
        }

        private string ProcessTableName(string tableName, string schema)
        {
            if (string.IsNullOrEmpty(tableName))
                return "";
            else
            {
                if (!tableName.StartsWith("`"))
                    tableName = $"`{tableName}`";

                if (!IsEmpty(out string s, schema, Schema))
                    if (!s.StartsWith("`"))
                        tableName = $"`{s}`.{tableName}";
                    else
                        tableName = $"{s}.{tableName}";

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            try
            {
                MySqlCommand cmd = new MySqlCommand(sql, conn, trans)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                reader = new IDataReaderWrapper(cmd.ExecuteReader());

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
                .Append("SELECT COUNT(*) AS \"_ROW_COUNT_\" FROM ")
                .Append(ProcessTableName(table.SourceName, table.SourceSchema));

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
            string tableName = ProcessTableName(table.SourceName, table.SourceSchema);

            // 语法格式形如：
            // SELECT <fieldsSQL> FROM <tableName> {WHERE <whereSQL>}
            // {ORDER BY <orderSQL>} LIMIT <fromRow - 1>, <toRow - fromRow + 1>
            //
            // 如果存在主键，可以优化为：
            // SELECT <A.fieldsSQL> FROM <tableName> A JOIN (SELECT <keyFields> FROM <tableName> {WHERE <whereSQL>}
            // {ORDER BY <orderSQL>} LIMIT <fromRow - 1>, <toRow - fromRow + 1>) B ON <A.keyFields> = <B.keyFields>
            //
            // 如果主键字段只有一个，可以进一步优化为：
            // SELECT <fieldsSQL> FROM <tableName> {WHERE {<keyField> > @LastMaxKey} {AND {<whereSQL>}}}
            // ORDER BY <keyField> ASC LIMIT <toRow - fromRow + 1>
            // 其中
            // @LastMaxKey = SELECT MAX(<keyField>) AS '_MaxKey_' FROM (SELECT <keyField> FROM <tableName>
            // {WHERE {<keyField> > @LastMaxKey} {AND {<whereSQL>}}} ORDER BY <keyField> ASC
            // LIMIT <toRow - fromRow + 1>) A
            if (table.KeyFields.Length == 1)
            {
                string keyField = ProcessFieldName(table.KeyFields[0]);
                string keyFieldWithPrefix = ProcessFieldName(table.KeyFields[0], tableName);

                // 查询最大键值
                sb.Append($"SELECT MAX({keyField}) AS '_MaxKey_' FROM (SELECT {keyFieldWithPrefix} FROM {tableName}");
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
                sb.Append($" ORDER BY {keyFieldWithPrefix} LIMIT {toRow - fromRow + 1}) A");

                if (QueryMaxKey(sb.ToString(), parms, out object maxValue))
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, tableName);

                    sb.Length = 0;
                    sb.Append($"SELECT {fieldsSQL} FROM {tableName}");
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
                    sb.Append($" ORDER BY {keyFieldWithPrefix} LIMIT {toRow - fromRow + 1}");

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

                    sb.Append($"SELECT {fieldsSQL} FROM {tableName} A JOIN (SELECT {keyFields} FROM {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" WHERE");
                        sb.Append($" {table.WhereSQL}");
                    }
                    if (!string.IsNullOrEmpty(table.OrderSQL))
                        sb.Append(" ORDER BY ").Append(table.OrderSQL);
                    sb.Append($" LIMIT {fromRow - 1}, {toRow - fromRow + 1}) B ON A.{keyField} = B.{keyField}");
                    for (int i = 1; i < table.KeyFields.Length; i++)
                    {
                        keyField = ProcessFieldName(table.KeyFields[i]);
                        sb.Append($" AND A.{keyField} = B.{keyField}");
                    }
                }
                else
                {
                    string fieldsSQL = ProcessFieldNames(table.SourceFields, tableName);

                    sb.Append($"SELECT {fieldsSQL} FROM {tableName}");
                    if (!string.IsNullOrEmpty(table.WhereSQL))
                    {
                        if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                            sb.Append(" WHERE");
                        sb.Append($" {table.WhereSQL}");
                    }
                    if (!string.IsNullOrEmpty(table.OrderSQL))
                        sb.Append(" ORDER BY ").Append(table.OrderSQL);
                    sb.Append($" LIMIT {fromRow - 1}, {toRow - fromRow + 1}");
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
}

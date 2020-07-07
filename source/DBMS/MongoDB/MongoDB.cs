using JHWork.DataMigration.Common;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;

namespace JHWork.DataMigration.DBMS.MongoDB
{
    /// <summary>
    /// MongoDB
    /// </summary>
    public class MongoDB : DBMSBase, IAssemblyLoader, IDBMSAssistant, IDBMSReader, IDBMSWriter
    {
        private const string IDField = "_id";
        private const string DocumentField = "document";

        private class BsonDocumentWrapper : IDataWrapper
        {
            private readonly List<BsonDocument> values;
            private int curRow = -1;
            private int[] indexMaps;
            private string[] names;

            public int FieldCount => indexMaps.Length;

            public int ReadCount { get; private set; } = 0;

            public BsonDocumentWrapper(List<BsonDocument> lst)
            {
                values = lst;

                ResetMap();
            }

            public void Close() { }

            public string GetFieldName(int i)
            {
                return names[i];
            }

            public string[] GetFieldNames()
            {
                return names;
            }

            public Type GetFieldType(int i)
            {
                return typeof(string);
            }

            private int GetMappedIndex(int i)
            {
                return indexMaps[i];
            }

            public object GetValue(int i)
            {
                if (curRow >= 0 && curRow < values.Count)
                {
                    i = GetMappedIndex(i);
                    if (i >= 0 && i < names.Length)
                    {
                        BsonValue bv = values[curRow][names[i]];

                        if (bv is BsonNull || bv == null)
                            return DBNull.Value;
                        else if (bv is BsonBoolean)
                            return bv.AsBoolean;
                        else
                            return bv.ToString();
                    }
                }

                return DBNull.Value;
            }

            public object GetValueByOriName(string field)
            {
                if (curRow >= 0 && curRow < values.Count)
                    if (DocumentField.Equals(field))
                        return values[curRow];
                    else if (IDField.Equals(field))
                        return values[curRow][IDField];

                return DBNull.Value;
            }

            public void MapFields(string[] fields)
            {
                if (fields == null || fields.Length == 0)
                    ResetMap();
                else
                {
                    indexMaps = new int[fields.Length];
                    names = fields;

                    // 对照
                    for (int i = 0; i < fields.Length; i++)
                        indexMaps[i] = i;

                }
            }

            public bool Read()
            {
                curRow++;
                if (curRow < values.Count)
                {
                    ReadCount++;
                    return true;
                }
                else
                    return false;
            }

            private void ResetMap()
            {
                indexMaps = new int[] { 0, 1 };
                names = new string[] { IDField, DocumentField };
            }
        }

        private class InsertScript
        {
            public List<BsonDocument> Data { get; } = new List<BsonDocument>();
        }

        private class UpdateScript
        {
            public string Filter { get; set; } = "";
            public BsonDocument Data { get; } = new BsonDocument();
        }

        private IMongoDatabase mdb = null;

        public bool BeginTransaction()
        {
            return true;
        }

        public bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            if (data.Read())
            {
                if (table is MaskingTable || table.WriteMode == WriteModes.Update)
                    BuildScriptWithUpdate(data, filter, out script);
                else
                    BuildScriptWithInsert(table, data, filter, out script);

                return true;
            }
            else
            {
                script = null;
                return false;
            }
        }

        private void BuildScriptWithInsert(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            InsertScript ins = new InsertScript();
            int r = 1;

            if (data.GetValueByOriName(DocumentField) is BsonDocument doc)
            {
                ins.Data.Add(doc);
                while (r < table.PageSize && data.Read())
                {
                    r++;

                    object obj = filter.GetValue(data, -1, DocumentField);

                    if (obj == DBNull.Value || obj == null)
                        doc = data.GetValueByOriName(DocumentField) as BsonDocument;
                    else
                        doc = obj as BsonDocument;

                    ins.Data.Add(doc);
                }
            }
            else
            {
                data.MapFields(null); // 取消字段映射
                doc = new BsonDocument();
                for (int i = 0; i < data.FieldCount; i++)
                    doc[data.GetFieldName(i)] = GetValue(filter.GetValue(data, i, data.GetFieldName(i)));
                ins.Data.Add(doc);
                while (r < table.PageSize && data.Read())
                {
                    r++;
                    doc = new BsonDocument();
                    for (int i = 0; i < data.FieldCount; i++)
                        doc[data.GetFieldName(i)] = GetValue(filter.GetValue(data, i, data.GetFieldName(i)));
                    ins.Data.Add(doc);
                }
            }

            script = ins;
        }

        private void BuildScriptWithUpdate(IDataWrapper data, IDataFilter filter, out object script)
        {
            UpdateScript upd = new UpdateScript();

            if (data.GetValueByOriName(DocumentField) is BsonDocument doc)
            {
                object obj = filter.GetValue(data, -1, DocumentField);

                if (obj != DBNull.Value && obj != null) doc = obj as BsonDocument;

                upd.Filter = $"{{\"{IDField}\":\"{doc["_id"]}\"}}";
                upd.Data.AddRange(doc);
                upd.Data.Remove(IDField);
            }
            else
                for (int i = 0; i < data.FieldCount; i++)
                {
                    string field = data.GetFieldName(i);

                    if (IDField.Equals(field))
                        upd.Filter = $"{{\"{IDField}\":\"{filter.GetValue(data, i, data.GetFieldName(i))}\"}}";
                    else
                        upd.Data[data.GetFieldName(i)] = GetValue(filter.GetValue(data, i, data.GetFieldName(i)));
                }

            script = upd;
        }

        public bool CommitTransaction()
        {
            return true;
        }

        public void Close()
        {
            mdb = null;
        }

        public bool Connect(Database db)
        {
            LogTitle = $"{db.Server}:{db.Port}/{db.DB}";
            try
            {
                MongoUrlBuilder mub = new MongoUrlBuilder()
                {
                    Server = new MongoServerAddress(db.Server, (int)db.Port),
                    Username = string.IsNullOrEmpty(db.User) ? null : db.User,
                    Password = string.IsNullOrEmpty(db.Pwd) ? null : db.Pwd,
                    UseTls = db.Encrypt,
                    MaxConnectionPoolSize = 1,
                    SocketTimeout = TimeSpan.FromSeconds(db.Timeout)
                };
                MongoClient mc = new MongoClient(mub.ToMongoUrl());

                mdb = mc.GetDatabase(db.DB);

                return mdb != null;
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
            IMongoCollection<BsonDocument> mc = mdb.GetCollection<BsonDocument>(table.DestName);

            if (script is InsertScript ins)
                try
                {
                    mc.InsertMany(ins.Data);
                    count = (uint)ins.Data.Count;
                    return true;
                }
                catch (Exception ex)
                {
                    LastError = ex.Message;
                    Logger.WriteLogExcept(LogTitle, ex);
                    count = 0;
                    return false;
                }
            else if (script is UpdateScript upd)
                try
                {
                    if (string.IsNullOrEmpty(upd.Filter))
                        mc.InsertOne(upd.Data);
                    else
                        mc.UpdateOne(upd.Filter, upd.Data);

                    count = 1;
                    return true;
                }
                catch (Exception ex)
                {
                    LastError = ex.Message;
                    Logger.WriteLogExcept(LogTitle, ex);
                    count = 0;
                    return false;
                }
            else
            {
                count = 0;
                return false;
            }
        }

        public bool GetFieldNames(string tableName, string schema, out string[] fieldNames)
        {
            fieldNames = new string[] { IDField, DocumentField };

            return true;
        }

        public string GetName()
        {
            return "MongoDB";
        }

        public DBMSParams GetParams()
        {
            return new DBMSParams()
            {
                Schema = false,
                CharSet = false,
                Compress = false
            };
        }

        protected override string[] GetTableKeys(string table, string schema)
        {
            return new string[] { IDField };
        }

        protected override string[] GetTableRefs(string table, string schema)
        {
            return new string[] { };
        }

        protected override string[] GetTables()
        {
            return mdb.ListCollectionNames().ToList().ToArray();
        }

        private BsonValue GetValue(object value)
        {
            if (value is DBNull || value == null)
                return BsonNull.Value;
            else if (value is string)
                return BsonString.Create(value);
            else if (value is bool)
                return BsonBoolean.Create(value);
            else if (value is int || value is short || value is byte)
                return BsonInt32.Create(value);
            else if (value is long)
                return BsonInt64.Create(value);
            else if (value is decimal)
                return BsonDecimal128.Create(value);
            else if (value is double || value is float)
                return BsonDouble.Create(value);
            else if (value is DateTime)
                return BsonDateTime.Create(value);
            else if (value is char c)
                return BsonString.Create("" + c);
            else if (value is byte[])
                return BsonBinaryData.Create(value);
            else
                return BsonString.Create(value.ToString());
        }

        public bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count)
        {
            try
            {
                IMongoCollection<BsonDocument> mc = mdb.GetCollection<BsonDocument>(table.SourceName);

                if (string.IsNullOrEmpty(table.WhereSQL))
                    count = (ulong)mc.CountDocuments(FilterDefinition<BsonDocument>.Empty);
                else
                {
                    string where = table.WhereSQL;

                    foreach (string key in parms.Keys)
                        where = where.Replace("@" + key, parms[key].ToString());

                    count = (ulong)mc.CountDocuments(where);
                }

                return true;
            }
            catch (Exception ex)
            {
                count = 0;
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            try
            {
                IMongoCollection<BsonDocument> mc = mdb.GetCollection<BsonDocument>(table.SourceName);

                if (string.IsNullOrEmpty(table.WhereSQL))
                    reader = new BsonDocumentWrapper(mc.Find(FilterDefinition<BsonDocument>.Empty)
                        .Skip((int)fromRow - 1).Limit((int)(toRow - fromRow) + 1).ToList());
                else
                {
                    string where = table.WhereSQL;

                    foreach (string key in parms.Keys)
                        where = where.Replace("@" + key, parms[key].ToString());

                    reader = new BsonDocumentWrapper(mc.Find(where)
                        .Skip((int)fromRow - 1).Limit((int)(toRow - fromRow) + 1).ToList());

                }

                return true;
            }
            catch (Exception ex)
            {
                reader = null;
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        public bool QueryParam(string sql, Dictionary<string, object> parms)
        {
            if (string.IsNullOrEmpty(sql)) return true;

            try
            {
                BsonDocument doc = mdb.RunCommand<BsonDocument>(sql);

                foreach (string name in doc.Names)
                    parms.Add(name, doc[name].ToString());

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        public bool RollbackTransaction()
        {
            return false;
        }
    }
}

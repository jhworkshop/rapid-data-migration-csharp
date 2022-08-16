using JHWork.DataMigration.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// MigrationRunner 的实例结构如下：
//
// MigrationInstance
//   + MigrationTask[] （串行）
//      + MigrationTable[][] （并行）
//
// 即，每个实例包含一组串行任务，每任务包含一组并行迁移表（二维数组结构用于解决表之间的依赖关系）。

namespace JHWork.DataMigration.Runner.Migration
{
    /// <summary>
    /// 迁移执行器，执行库与库之间的数据迁移。
    /// </summary>
    public class MigrationRunner : RunnerBase, IAssemblyLoader, IRunnerAnalyzer, IRunnerExecutor, IRunnerAssistant
    {
        private IStopStatus status;

        private void AnalyseDatabase(JObject obj, JObject inherited, Database db, string prefix)
        {
            db.DBMS = GetJValue(obj, inherited, "dbms", prefix);
            db.Server = GetJValue(obj, inherited, "server", prefix);
            db.Port = uint.Parse(GetJValue(obj, inherited, "port", prefix));
            db.Schema = GetJValue(obj, inherited, "schema", prefix);
            db.User = GetJValue(obj, inherited, "user", prefix);
            db.Pwd = GetJValue(obj, inherited, "password", prefix);
            db.CharSet = GetJValue(obj, inherited, "charset", prefix, "utf8");
            db.Encrypt = int.Parse(GetJValue(obj, inherited, "encrypt", prefix, "0")) != 0;
            db.Compress = int.Parse(GetJValue(obj, inherited, "compress", prefix, "0")) != 0;
            db.Timeout = uint.Parse(GetJValue(obj, inherited, "timeout", prefix, "60"));
        }

        private void AnalyseDatabase(string db, MigrationTask task)
        {
            string[] dbs = db.Split(',');
            string dbName = Database.AnalyseDB(dbs[0]);
            string schema = Database.AnalyseSchema(dbs[0]);

            task.Source.DB = dbName;
            if (!string.IsNullOrEmpty(schema)) task.Source.Schema = schema;

            if (dbs.Length > 1)
            {
                db = dbs[1];
                dbName = Database.AnalyseDB(db);
                schema = Database.AnalyseSchema(db);
            }
            task.Dest.DB = dbName;
            if (!string.IsNullOrEmpty(schema)) task.Dest.Schema = schema;
        }

        public Instance[] AnalyseInstance(JArray objs, JObject inherited, string path)
        {
            List<Instance> instances = new List<Instance>();

            for (int i = 0; i < objs.Count; i++)
            {
                MigrationInstance ins = new MigrationInstance();
                JObject obj = objs[i] as JObject;
                JArray dbs = obj["dbs"] as JArray;
                List<MigrationTask> tasks = new List<MigrationTask>();

                for (int j = 0; j < dbs.Count; j++)
                {
                    MigrationTask task = new MigrationTask();

                    AnalyseTask(obj, inherited, dbs[j].ToString(), task, path);
                    tasks.Add(task);
                }

                ins.Tasks = tasks.ToArray();

                if (tasks.Count > 0)
                {
                    ins.Name = $"{tasks[0].Source.Server}:{tasks[0].Source.Port} -> {tasks[0].Dest.Server}:{tasks[0].Dest.Port}";
                    instances.Add(ins);
                }
            }

            return instances.ToArray();
        }

        private void AnalyseTable(string file, MigrationTask task)
        {
            JObject obj = LoadAndDeserialize(file);
            JArray tables = obj["tables"] as JArray;
            List<MigrationTable> buf = new List<MigrationTable>();

            task.Params = obj["params"].ToString();

            // #1: 获取所有表
            for (int i = 0; i < tables.Count; i++)
            {
                JObject o = tables[i] as JObject;
                string[] names = o["name"].ToString().Split(',');
                MigrationTable table = new MigrationTable()
                {
                    SourceName = Table.AnalyseName(names[0]),
                    SourceSchema = Table.AnalyseSchema(names[0]),
                    DestName = Table.AnalyseName(names.Length > 1 ? names[1] : names[0]),
                    DestSchema = Table.AnalyseSchema(names.Length > 1 ? names[1] : names[0]),
                    Order = int.Parse(o["order"].ToString()),
                    OrderSQL = o["orderSQL"].ToString(),
                    WhereSQL = o["whereSQL"].ToString(),
                    PageSize = uint.Parse(o["pageSize"].ToString()),
                    WriteMode = "UPDATE".Equals(o["mode"].ToString().ToUpper()) ? WriteModes.Update : WriteModes.Append,
                    KeyFields = o["keyFields"].ToString().Length > 0 ? o["keyFields"].ToString().Split(',') : new string[0],
                    SkipFields = o["skipFields"].ToString().Split(','),
                    Filter = o["filter"].ToString(),
                    KeepIdentity = true,
                    DestFields = new string[] { },
                    References = o.ContainsKey("references") ? o["references"].ToString().Split(',') : new string[] { },
                    Total = 0,
                    Progress = 0,
                    Status = DataStates.Idle
                };

                buf.Add(table);

                if (table.WriteMode == WriteModes.Update && (table.KeyFields.Length == 0 || "".Equals(table.KeyFields[0])))
                    throw new Exception($"表 {table.SourceName} 配置有误！更新模式必须指定主键字段(keyFields)。");
                if ("".Equals(table.OrderSQL))
                    throw new Exception($"表 {table.SourceName} 配置有误！必须指定稳定的排序规则(orderSQL)。");
                if (table.PageSize <= 0)
                    throw new Exception($"表 {table.SourceName} 配置有误！批量记录数必须大于零(pageSize)。");
            }

            // #2: 生成结构
            if (buf.Count > 0)
            {
                buf.Sort(new TableComparer());

                int order = buf[0].Order;
                List<MigrationTable> tmpBuf = new List<MigrationTable>() { buf[0] };
                List<List<MigrationTable>> rstBuf = new List<List<MigrationTable>>() { tmpBuf };

                for (int i = 1; i < buf.Count; i++)
                {
                    if (buf[i].Order != order)
                    {
                        tmpBuf = new List<MigrationTable>() { buf[i] };
                        rstBuf.Add(tmpBuf);
                        order = buf[i].Order;
                    }
                    else
                        tmpBuf.Add(buf[i]);
                }

                task.Tables = new MigrationTable[rstBuf.Count][];
                for (int i = 0; i < task.Tables.Length; i++)
                    task.Tables[i] = rstBuf[i].ToArray();
            }
            else
                task.Tables = new MigrationTable[][] { };
        }

        private void AnalyseTask(JObject obj, JObject inherited, string db, MigrationTask task, string path)
        {
            task.ReadPages = uint.Parse(GetJValue(obj, inherited, "readPages"));
            task.Threads = uint.Parse(GetJValue(obj, inherited, "threads"));
            task.Progress = 0;
            task.Total = 0;
            task.Status = DataStates.Idle;
            task.StartTick = 0;

            if (!(task.ReadPages > 0))
                throw new Exception("每次读取数据页数必须大于零(readPages)。");
            if (!(task.Threads > 0))
                throw new Exception("并发迁移表数必须大于零(threads)。");

            AnalyseDatabase(obj["source"] as JObject, inherited, task.Source, "source");
            AnalyseDatabase(obj["dest"] as JObject, inherited, task.Dest, "dest");
            AnalyseDatabase(db, task);
            AnalyseTable($"{path}\\{GetJValue(obj, inherited, "tables")}", task);

            task.Name = $"{task.Source.DB} -> {task.Dest.DB}";
        }

        public void Execute(Instance ins, IStopStatus status, bool withTrans)
        {
            this.status = status;

            foreach (Common.Task t in ins.Tasks)
            {
                if (status.Stopped) break;

                if (t is MigrationTask task && task.Tables.Length > 0)
                {
                    task.StartTick = WinAPI.GetTickCount();
                    task.Status = DataStates.Running;

                    // 构建待迁移表清单
                    List<List<MigrationTable>> lst = new List<List<MigrationTable>>();
                    TableComparer comparer = new TableComparer();

                    for (int i = 0; i < task.Tables.Length; i++)
                    {
                        List<MigrationTable> tables = new List<MigrationTable>(task.Tables[i]);

                        tables.Sort(comparer);
                        lst.Add(tables);
                    }

                    // 开始迁移
                    try
                    {
                        foreach (List<MigrationTable> tables in lst)
                            Parallel.ForEach(CreateThreadAction((int)task.Threads), _ =>
                            {
                                MigrationTable table = GetTable(tables);

                                while (table != null)
                                {
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.DestName}", "迁移开始...");

                                    MigrateTable(task, table, withTrans, out string reason);
                                    if (table.Status == DataStates.Done)
                                    {
                                        Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.DestName}", "迁移成功。");
                                        Logger.WriteRpt(task.Dest.Server, task.Dest.DB, table.DestName, "成功",
                                            table.Progress.ToString("#,##0"));
                                    }
                                    else
                                    {
                                        task.Status = DataStates.RunningError;
                                        task.ErrorMsg = reason;
                                        task.Progress -= table.Progress;
                                        Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.DestName}",
                                            $"迁移失败！{reason}");
                                        Logger.WriteRpt(task.Dest.Server, task.Dest.DB, table.DestName, "失败", reason);
                                    }
                                    table = GetTable(tables);
                                }
                            });

                        if (status.Stopped || task.Status == DataStates.RunningError || task.Status == DataStates.Error)
                        {
                            task.Status = DataStates.Error;
                            Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", "迁移失败！");
                        }
                        else
                        {
                            task.Status = DataStates.Done;
                            Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", "迁移成功！");
                        }
                    }
                    catch (Exception ex)
                    {
                        task.Status = DataStates.Error;
                        task.ErrorMsg = ex.Message;
                        Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", $"迁移失败！{ex.Message}");
                    }
                    task.StartTick = WinAPI.GetTickCount() - task.StartTick;
                }
            }
        }

        public string GetName()
        {
            return "Migration";
        }

        private MigrationTable GetTable(List<MigrationTable> tables)
        {
            lock (tables)
            {
                if (tables.Count > 0)
                {
                    MigrationTable mt = tables[0];

                    tables.RemoveAt(0);

                    return mt;
                }
                else
                    return null;
            }
        }

        public void LoadSample(Instance ins, Database source, Database dest, List<Table> tables, out string param)
        {
            if (ins.Tasks[0] is MigrationTask task)
            {
                source.Duplicate(task.Source);
                dest.Duplicate(task.Dest);

                foreach (Table[] t in task.Tables)
                    tables.AddRange(t);

                param = task.Params;
            }
            else
                param = "";
        }

        private void MigrateTable(MigrationTask task, MigrationTable table, bool withTrans, out string reason)
        {
            reason = "取消操作";
            if (status.Stopped) return;

            if (Connect(task, task.Source, out IDBMSReader source, task.Dest, out IDBMSWriter dest))
            {
                Dictionary<string, object> parms = new Dictionary<string, object>();

                dest.QueryParam(task.Params, parms);
                if (withTrans) dest.BeginTransaction();
                try
                {
                    // 迁移数据
                    MigrateTableWithScript(task, table, parms, source, dest, out reason);
                    if (table.Status != DataStates.Error && !status.Stopped)
                    {
                        if (withTrans) dest.CommitTransaction();
                        table.Status = DataStates.Done;
                    }
                    else
                    {
                        if (withTrans) dest.RollbackTransaction();
                        table.Status = DataStates.Error;
                    }
                }
                catch (Exception ex)
                {
                    if (withTrans) dest.RollbackTransaction();
                    table.Status = DataStates.Error;
                    reason = ex.Message;
                }

                source.Close();
                dest.Close();
            }
            else
            {
                table.Status = DataStates.Error;
                reason = "连接失败！";
            }
        }

        private void MigrateTableWithScript(MigrationTask task, MigrationTable table, Dictionary<string, object> parms,
            IDBMSReader source, IDBMSWriter dest, out string failReason)
        {
            ConcurrentQueue<object> scripts = new ConcurrentQueue<object>();
            bool read = false;
            string reason = "";

            Parallel.ForEach(CreateThreadAction(), act =>
            {
                try
                {
                    if ("read".Equals(act))
                    {
                        uint bufSize = task.ReadPages * 3;
                        uint fromRow = 1, toRow = task.ReadPages * table.PageSize;
                        IDataFilter filter = DataFilterFactory.GetFilterByName(table.Filter);
                        IDataWrapper data = null;

                        while (true)
                        {
                            while (scripts.Count > bufSize && !status.Stopped && table.Status != DataStates.Error)
                                Thread.Sleep(50);

                            if (status.Stopped || table.Status == DataStates.Error) break;

                            if (source.QueryPage(table, fromRow, toRow, WithEnums.NoLock, parms, out data))
                                try
                                {
                                    object script = null;

                                    data.MapFields(table.DestFields);
                                    while (dest.BuildScript(table, data, filter, out script)
                                        && !status.Stopped && table.Status != DataStates.Error)
                                        scripts.Enqueue(script);

                                    // 获取不到预期的记录数，作最后一页处理
                                    if (data.ReadCount != task.ReadPages * table.PageSize || status.Stopped) break;
                                }
                                finally
                                {
                                    data.Close();
                                }
                            else
                            {
                                table.Status = DataStates.Error;
                                reason = source.LastError;
                                break;
                            }

                            fromRow = toRow + 1;
                            toRow += task.ReadPages * table.PageSize;
                        }

                        read = true;
                    }
                    else if ("write".Equals(act))
                    {
                        while (table.Status != DataStates.Error && (!read || scripts.Count > 0) && !status.Stopped)
                            if (scripts.Count > 0)
                            {
                                scripts.TryDequeue(out object script);
                                if (!dest.ExecScript(table, script, out uint r))
                                {
                                    table.Status = DataStates.Error;
                                    reason = dest.LastError;
                                    break;
                                }

                                lock (task)
                                {
                                    table.Progress += r;
                                    task.Progress += r;
                                }
                            }
                            else
                                Thread.Sleep(1);
                    }
                }
                catch (Exception ex)
                {
                    table.Status = DataStates.Error;
                    reason = ex.Message;
                }
            });

            failReason = table.Status == DataStates.Error ? reason : "";
        }

        public void Prefetch(Instance ins, IStopStatus status)
        {
            this.status = status;

            foreach (Common.Task t in ins.Tasks)
            {
                if (status.Stopped) break;

                if (t is MigrationTask task)
                {
                    task.Progress = 0;
                    task.Total = 0;
                    task.Status = DataStates.Running;
                    if (Connect(task, task.Source, out IDBMSReader source, task.Dest, out IDBMSWriter dest))
                    {
                        Dictionary<string, object> parms = new Dictionary<string, object>();

                        dest.QueryParam(task.Params, parms);

                        for (int i = 0; i < task.Tables.Length; i++)
                            for (int j = 0; j < task.Tables[i].Length; j++)
                                if (!status.Stopped)
                                {
                                    MigrationTable table = task.Tables[i][j];

                                    table.Status = DataStates.Running;
                                    PrefetchTable(task, table, parms, source, dest);
                                    if (table.Status == DataStates.Error)
                                        task.Status = DataStates.RunningError;
                                    else
                                        table.Status = DataStates.Idle;
                                }

                        if (task.Status != DataStates.Error && task.Status != DataStates.RunningError)
                            task.Status = DataStates.Idle;
                        source.Close();
                        dest.Close();
                    }
                    else
                        task.Status = DataStates.Error;
                }
            }
        }

        private void PrefetchTable(MigrationTask task, MigrationTable table, Dictionary<string, object> parms,
            IDBMSReader source, IDBMSWriter dest)
        {
            Parallel.ForEach(CreateThreadAction(), act =>
            {
                if ("read".Equals(act))
                {
                    bool isError = true;

                    // #1: 检查表存在，并缓存字段
                    if (source.GetFieldNames(table.SourceName, table.SourceSchema, out string[] fields))
                    {
                        table.SourceFields = fields;

                        // #2: 获取待迁移记录数
                        if (source.QueryCount(table, WithEnums.NoLock, parms, out ulong count))
                        {
                            table.Progress = 0;
                            task.Total += count;
                            table.Total = count;
                            isError = false;
                        }
                    }

                    if (isError)
                    {
                        table.Status = DataStates.Error;
                        task.ErrorMsg = source.LastError;
                    }
                }
                else if ("write".Equals(act))
                {
                    if (dest.GetFieldNames(table.DestName, table.DestSchema, out string[] fields))
                        table.DestFields = fields;
                    else
                    {
                        table.Status = DataStates.Error;
                        task.ErrorMsg = dest.LastError;
                    }
                }
            });
        }

        public void Reset(Instance ins)
        {
            foreach (Common.Task t in ins.Tasks)
                if (t is MigrationTask task)
                {
                    task.Progress = 0;
                    task.StartTick = 0;
                    task.Status = DataStates.Idle;
                    task.Total = 0;

                    for (int i = 0; i < task.Tables.Length; i++)
                        foreach (MigrationTable table in task.Tables[i])
                        {
                            table.Total = 0;
                            table.Progress = 0;
                            table.Status = DataStates.Idle;
                        }
                }
        }

        public void SaveSample(Database source, Database dest, List<Table> tables, string param, string path,
            string file)
        {
            // 表配置
            JArray tableArray = new JArray();

            foreach (Table t in tables)
            {
                JObject obj = new JObject()
                {
                    ["name"] = t.SourceFullName.Equals(t.DestFullName) ? t.SourceFullName : $"{t.SourceFullName},{t.DestFullName}",
                    ["order"] = t.Order,
                    ["orderSQL"] = t.OrderSQL,
                    ["whereSQL"] = t.WhereSQL,
                    ["pageSize"] = t.PageSize,
                    ["mode"] = t.WriteMode == WriteModes.Append ? "Append" : "Update",
                    ["keyFields"] = t.KeyFields == null ? "" : string.Join(",", t.KeyFields),
                    ["skipFields"] = t.SkipFields == null ? "" : string.Join(",", t.SkipFields),
                    ["filter"] = t.Filter,
                    ["References"] = t.References == null ? "" : string.Join(",", t.References)
                };

                tableArray.Add(obj);
            }

            JObject tableData = new JObject()
            {
                ["params"] = param,
                ["tables"] = tableArray
            };

            WriteFile($"{path}Table-{file}", JsonConvert.SerializeObject(tableData, Formatting.Indented));

            // 任务配置
            JObject srcDB = new JObject()
            {
                ["dbms"] = source.DBMS,
                ["server"] = source.Server,
                ["port"] = source.Port,
                ["schema"] = source.Schema,
                ["user"] = source.User,
                ["password"] = source.Pwd,
                ["charset"] = source.CharSet,
                ["compress"] = source.Compress ? 1 : 0,
                ["encrypt"] = source.Encrypt ? 1 : 0,
                ["timeout"] = source.Timeout
            };
            JObject dstDB = new JObject()
            {
                ["dbms"] = dest.DBMS,
                ["server"] = dest.Server,
                ["port"] = dest.Port,
                ["schema"] = dest.Schema,
                ["user"] = dest.User,
                ["password"] = dest.Pwd,
                ["charset"] = dest.CharSet,
                ["compress"] = dest.Compress ? 1 : 0,
                ["encrypt"] = dest.Encrypt ? 1 : 0,
                ["timeout"] = dest.Timeout
            };
            JArray dbArray = new JArray();
            if (source.DB.Equals(dest.DB))
                dbArray.Add(source.DB);
            else
                dbArray.Add($"{source.DB},{dest.DB}");

            JObject task = new JObject()
            {
                ["source"] = srcDB,
                ["dest"] = dstDB,
                ["dbs"] = dbArray,
                ["tables"] = $"Table-{file}",
                ["readPages"] = 10,
                ["threads"] = 1
            };
            JArray taskArray = new JArray() { task };
            JObject profileData = new JObject()
            {
                ["instances"] = taskArray,
                ["mode"] = "Once",
                ["runner"] = "Migration",
                ["runtime"] = "00:00",
                ["threads"] = 1
            };

            WriteFile($"{path}Profile-{file}", JsonConvert.SerializeObject(profileData, Formatting.Indented));
        }
    }
}

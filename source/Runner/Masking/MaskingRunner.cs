using JHWork.DataMigration.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// MaskingRunner 的实例结构如下：
//
// MaskingInstance
//   + MaskingTask[] （串行）
//       + MaskingTable[] （并行）
//
// 即，每个实例包含一组串行任务，每任务包含一组并行表。

namespace JHWork.DataMigration.Runner.Masking
{
    /// <summary>
    /// 脱敏执行器，执行数据脱敏任务。
    /// </summary>
    public class MaskingRunner : RunnerBase, IAssemblyLoader, IRunnerAnalyzer, IRunnerExecutor, IRunnerAssistant
    {
        private IStopStatus status;

        private void AnalyseDatabase(JObject obj, JObject inherited, Database db, string sdb, string prefix)
        {
            string schema = Database.AnalyseSchema(sdb);

            db.DBMS = GetJValue(obj, inherited, "dbms", prefix);
            db.Server = GetJValue(obj, inherited, "server", prefix);
            db.Port = uint.Parse(GetJValue(obj, inherited, "port", prefix));
            db.Schema = GetJValue(obj, inherited, "schema", prefix);
            db.DB = Database.AnalyseDB(sdb);
            if (!string.IsNullOrEmpty(schema)) db.Schema = schema;
            db.User = GetJValue(obj, inherited, "user", prefix);
            db.Pwd = GetJValue(obj, inherited, "password", prefix);
            db.CharSet = GetJValue(obj, inherited, "charset", prefix, "utf8");
            db.Encrypt = int.Parse(GetJValue(obj, inherited, "encrypt", prefix, "0")) != 0;
            db.Compress = int.Parse(GetJValue(obj, inherited, "compress", prefix, "0")) != 0;
            db.Timeout = uint.Parse(GetJValue(obj, inherited, "timeout", prefix, "60"));
        }

        public Instance[] AnalyseInstance(JArray objs, JObject inherited, string path)
        {
            List<Instance> instances = new List<Instance>();

            for (int i = 0; i < objs.Count; i++)
            {
                MaskingInstance ins = new MaskingInstance();
                JObject obj = objs[i] as JObject;
                JArray dbs = obj["dbs"] as JArray;
                List<MaskingTask> tasks = new List<MaskingTask>();

                for (int j = 0; j < dbs.Count; j++)
                {
                    MaskingTask task = new MaskingTask();

                    AnalyseTask(obj, inherited, dbs[j].ToString(), task, path);
                    tasks.Add(task);
                }

                ins.Tasks = tasks.ToArray();

                if (tasks.Count > 0)
                {
                    ins.Name = $"{tasks[0].Dest.Server}:{tasks[0].Dest.Port}";
                    instances.Add(ins);
                }
            }

            return instances.ToArray();
        }

        private void AnalyseTable(string file, MaskingTask task)
        {
            JObject obj = LoadAndDeserialize(file);
            JArray tables = obj["tables"] as JArray;
            List<MaskingTable> buf = new List<MaskingTable>();

            task.Params = obj["params"].ToString();

            // #1: 获取所有表
            for (int i = 0; i < tables.Count; i++)
            {
                JObject o = tables[i] as JObject;
                string name = o["name"].ToString();
                MaskingTable table = new MaskingTable()
                {
                    SourceName = Table.AnalyseName(name),
                    SourceSchema = Table.AnalyseSchema(name),
                    DestName = Table.AnalyseName(name),
                    DestSchema = Table.AnalyseSchema(name),
                    Order = int.Parse(o["order"].ToString()),
                    OrderSQL = o["orderSQL"].ToString(),
                    WhereSQL = o["whereSQL"].ToString(),
                    PageSize = uint.Parse(o["pageSize"].ToString()),
                    WriteMode = WriteModes.Append,
                    KeyFields = o["keyFields"].ToString().Length > 0 ? o["keyFields"].ToString().Split(',') : new string[0],
                    SkipFields = new string[] { },
                    MaskFields = o["maskFields"].ToString().Split(','),
                    Filter = o["filter"].ToString(),
                    KeepIdentity = false,
                    References = new string[] { },
                    Total = 0,
                    Progress = 0,
                    Status = DataStates.Idle
                };
                List<string> destFields = new List<string>();

                destFields.AddRange(table.KeyFields);
                destFields.AddRange(table.MaskFields);
                table.DestFields = destFields.ToArray();

                buf.Add(table);

                if (table.WriteMode == WriteModes.Update && (table.KeyFields.Length == 0 || "".Equals(table.KeyFields[0])))
                    throw new Exception($"表 {table.SourceName} 配置有误！更新模式必须指定主键字段(keyFields)。");
                if ("".Equals(table.OrderSQL))
                    throw new Exception($"表 {table.SourceName} 配置有误！必须指定稳定的排序规则(orderSQL)。");
                if (table.PageSize <= 0)
                    throw new Exception($"表 {table.SourceName} 配置有误！批量记录数必须大于零(pageSize)。");
                if (string.IsNullOrEmpty(table.Filter))
                    throw new Exception($"表 {table.SourceName} 配置有误！必须指定过滤器(filter)。");
            }

            task.Tables = buf.ToArray();
        }

        private void AnalyseTask(JObject obj, JObject inherited, string db, MaskingTask task, string path)
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
                throw new Exception("并发脱敏表数必须大于零(threads)。");

            AnalyseDatabase(obj["dest"] as JObject, inherited, task.Dest, db, "dest");
            AnalyseTable($"{path}\\{GetJValue(obj, inherited, "tables")}", task);

            task.Name = $"{task.Dest.DB}";
        }

        public void Execute(Instance ins, IStopStatus status, bool withTrans)
        {
            this.status = status;

            foreach (Common.Task t in ins.Tasks)
            {
                if (status.Stopped) break;

                if (t is MaskingTask task && task.Tables.Length > 0)
                {
                    task.StartTick = WinAPI.GetTickCount();
                    task.Status = DataStates.Running;

                    // 构建待脱敏表清单
                    List<MaskingTable> lst = new List<MaskingTable>();
                    TableComparer comparer = new TableComparer();

                    foreach (Table tt in task.Tables)
                        if (tt is MaskingTable table) lst.Add(table);
                    lst.Sort(comparer);

                    // 开始脱敏
                    try
                    {
                        Parallel.ForEach(CreateThreadAction((int)task.Threads), _ =>
                        {
                            MaskingTable table = GetTable(lst);

                            while (table != null)
                            {
                                Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.SourceName}", "脱敏开始...");

                                MaskTable(task, table, withTrans, out string reason);
                                if (table.Status == DataStates.Done)
                                {
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.SourceName}", "脱敏成功。");
                                    Logger.WriteRpt(task.Dest.Server, task.Dest.DB, table.SourceName, "成功",
                                        table.Progress.ToString("#,##0"));
                                }
                                else
                                {
                                    task.Status = DataStates.RunningError;
                                    task.ErrorMsg = reason;
                                    task.Progress -= table.Progress;
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.SourceName}",
                                        $"脱敏失败！{reason}");
                                    Logger.WriteRpt(task.Dest.Server, task.Dest.DB, table.SourceName, "失败", reason);
                                }

                                table = GetTable(lst);
                            }
                        });

                        if (status.Stopped || task.Status == DataStates.RunningError || task.Status == DataStates.Error)
                        {
                            task.Status = DataStates.Error;
                            Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", "脱敏失败！");
                        }
                        else
                        {
                            task.Status = DataStates.Done;
                            Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", "脱敏成功！");
                        }
                    }
                    catch (Exception ex)
                    {
                        task.Status = DataStates.Error;
                        task.ErrorMsg = ex.Message;
                        Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", $"脱敏失败！{ex.Message}");
                    }
                    task.StartTick = WinAPI.GetTickCount() - task.StartTick;
                }
            }
        }

        public string GetName()
        {
            return "Masking";
        }

        private MaskingTable GetTable(List<MaskingTable> lst)
        {
            lock (lst)
            {
                if (lst.Count > 0)
                {
                    MaskingTable rst = lst[0];

                    lst.RemoveAt(0);

                    return rst;
                }
                else
                    return null;
            }
        }

        public void LoadSample(Instance ins, Database source, Database dest, List<Table> tables, out string param)
        {
            if (ins.Tasks[0] is MaskingTask task)
            {
                source.Duplicate(task.Dest);
                dest.Duplicate(task.Dest);

                tables.AddRange(task.Tables);

                param = task.Params;
            }
            else
                param = "";
        }

        private void MaskTable(MaskingTask task, MaskingTable table, bool withTrans, out string reason)
        {
            reason = "取消操作";
            if (status.Stopped) return;

            if (Connect(task, task.Dest, out IDBMSReader source, task.Dest, out IDBMSWriter dest))
            {
                Dictionary<string, object> parms = new Dictionary<string, object>();

                dest.QueryParam(task.Params, parms);
                if (withTrans) dest.BeginTransaction();
                try
                {
                    // 脱敏数据
                    MaskTableWithScript(task, table, parms, source, dest, out reason);
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

        private void MaskTableWithScript(MaskingTask task, MaskingTable table, Dictionary<string, object> parms,
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

                if (t is MaskingTask task)
                {
                    task.Progress = 0;
                    task.Total = 0;
                    task.Status = DataStates.Running;
                    if (Connect(task, task.Dest, out IDBMSReader source, task.Dest, out IDBMSWriter dest))
                    {
                        Dictionary<string, object> parms = new Dictionary<string, object>();

                        dest.QueryParam(task.Params, parms);

                        for (int i = 0; i < task.Tables.Length; i++)
                            if (!status.Stopped)
                            {
                                task.Tables[i].Status = DataStates.Running;
                                PrefetchTable(task, task.Tables[i], parms, source);
                                if (task.Tables[i].Status == DataStates.Error)
                                    task.Status = DataStates.RunningError;
                                else
                                    task.Tables[i].Status = DataStates.Idle;
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

        private void PrefetchTable(MaskingTask task, MaskingTable table, Dictionary<string, object> parms,
            IDBMSReader source)
        {
            bool isError = true;

            // #1: 检查表存在，并缓存字段
            if (source.GetFieldNames(table.SourceName, table.SourceSchema, out string[] fields))
            {
                table.SourceFields = fields;

                // #2: 获取待脱敏记录数
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

        public void Reset(Instance ins)
        {
            foreach (Common.Task t in ins.Tasks)
            {
                if (t is MaskingTask task)
                {
                    task.Progress = 0;
                    task.StartTick = 0;
                    task.Status = DataStates.Idle;
                    task.Total = 0;

                    foreach (MaskingTable table in task.Tables)
                    {
                        table.Progress = 0;
                        table.Status = DataStates.Idle;
                        table.Total = 0;
                    }
                }
            }
        }

        public void SaveSample(Database source, Database dest, List<Table> tables, string param, string path, string file)
        {
            // 表配置
            JArray tableArray = new JArray();

            foreach (Table t in tables)
            {
                JObject obj = new JObject()
                {
                    ["name"] = t.DestFullName,
                    ["order"] = t.Order,
                    ["orderSQL"] = t.OrderSQL,
                    ["whereSQL"] = t.WhereSQL,
                    ["pageSize"] = t.PageSize,
                    ["keyFields"] = t.KeyFields == null ? "" : string.Join(",", t.KeyFields),
                    ["maskFields"] = t.DestFields == null ? "" : string.Join(",", t.DestFields),
                    ["filter"] = t.Filter
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
            JObject destDB = new JObject()
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
                ["timeout"] = dest.Timeout,
            };
            JArray dbArray = new JArray
            {
                dest.DB
            };

            JObject task = new JObject()
            {
                ["dest"] = destDB,
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
                ["runner"] = "Masking",
                ["runtime"] = "00:00",
                ["threads"] = 1
            };

            WriteFile($"{path}Profile-{file}", JsonConvert.SerializeObject(profileData, Formatting.Indented));
        }
    }
}

using JHWork.DataMigration.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
    public class MigrationRunner : IAssemblyLoader, IRunnerAnalyzer, IRunnerExecutor, IRunnerAssistant
    {
        private IStopStatus status;

        private void AnalyseDatabase(JObject obj, Database db)
        {
            db.DBMS = obj["dbms"].ToString();
            db.Server = obj["server"].ToString();
            db.Port = uint.Parse(obj["port"].ToString());
            db.User = obj["user"].ToString();
            db.Pwd = obj.ContainsKey("password") ? obj["password"].ToString() : "";
            db.CharSet = obj.ContainsKey("charset") ? obj["charset"].ToString() : "utf8";
            db.Encrypt = obj.ContainsKey("encrypt") ? int.Parse(obj["encrypt"].ToString()) != 0 : false;
            db.Compress = obj.ContainsKey("compress") ? int.Parse(obj["compress"].ToString()) != 0 : false;
        }

        private void AnalyseDatabase(string db, MigrationTask task)
        {
            string[] s = db.Split(',');

            task.Source.DB = s[0];
            task.Dest.DB = s.Length > 1 ? s[1] : s[0];
        }

        public Instance[] AnalyseInstance(JArray objs, string path)
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

                    AnalyseTask(obj, dbs[j].ToString(), task, path);
                    tasks.Add(task);
                }

                ins.Tasks = tasks.ToArray();

                if (tasks.Count > 0)
                {
                    ins.Name = $"{tasks[0].Source.Server} -> {tasks[0].Dest.Server}";
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
                    SourceName = names[0],
                    DestName = names.Length > 1 ? names[1] : names[0],
                    Order = int.Parse(o["order"].ToString()),
                    OrderSQL = o["orderSQL"].ToString(),
                    SourceWhereSQL = o["whereSQL"].ToString(),
                    DestWhereSQL = o.ContainsKey("destWhereSQL") ? o["destWhereSQL"].ToString()
                        : o["whereSQL"].ToString(),
                    PageSize = uint.Parse(o["pageSize"].ToString()),
                    WriteMode = "UPDATE".Equals(o["mode"].ToString().ToUpper()) ? WriteModes.Update
                        : WriteModes.Append,
                    KeyFields = o["keyFields"].ToString().Split(','),
                    SkipFields = o["skipFields"].ToString().Split(','),
                    Filter = o["filter"].ToString(),
                    KeepIdentity = o.ContainsKey("dest.mssql.keepIdentity") ?
                        int.Parse(o["dest.mssql.keepIdentity"].ToString()) != 0 : true,
                    DestFields = new string[] { },
                    References = o.ContainsKey("references") ? o["references"].ToString().Split(',') : new string[] { },
                    Total = 0,
                    Progress = 0,
                    Status = DataStates.Idle
                };

                buf.Add(table);

                if (table.WriteMode == WriteModes.Update && "".Equals(table.KeyFields[0]))
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

        private void AnalyseTask(JObject obj, string db, MigrationTask task, string path)
        {
            task.ReadPages = uint.Parse(obj["readPages"].ToString());
            task.Threads = uint.Parse(obj["threads"].ToString());
            task.Progress = 0;
            task.Total = 0;
            task.Status = DataStates.Idle;
            task.StartTick = 0;

            if (!(task.ReadPages > 0))
                throw new Exception("每次读取数据页数必须大于零(readPages)。");
            if (!(task.Threads > 0))
                throw new Exception("并发迁移表数必须大于零(threads)。");

            AnalyseDatabase(obj["source"] as JObject, task.Source);
            AnalyseDatabase(obj["dest"] as JObject, task.Dest);
            AnalyseDatabase(db, task);
            AnalyseTable($"{path}\\{obj["tables"]}", task);

            task.Name = $"{task.Source.DB} -> {task.Dest.DB}";
        }

        private bool Connect(Database db, out IDBMSReader reader)
        {
            reader = DBMSFactory.GetDBMSReaderByName(db.DBMS);
            if (reader == null)
            {
                Logger.WriteLog("系统", $"数据库类型 {db.DBMS} 不支持！");
                return false;
            }
            else
                return reader.Connect(db);
        }

        private bool Connect(Database db, out IDBMSWriter writer)
        {
            writer = DBMSFactory.GetDBMSWriterByName(db.DBMS);
            if (writer == null)
            {
                Logger.WriteLog("系统", $"数据库类型 {db.DBMS} 不支持！");
                return false;
            }
            else
                return writer.Connect(db);
        }

        private string[] CreateThreadAction()
        {
            return new string[] { "read", "write" };
        }

        private int[] CreateThreadAction(int count)
        {
            int[] rst = new int[count];

            for (int i = 0; i < count; i++)
                rst[i] = i + 1;

            return rst;
        }

        private void DuplicateDatabase(Database source, Database dest)
        {
            dest.DBMS = source.DBMS;
            dest.Server = source.Server;
            dest.Port = source.Port;
            dest.DB = source.DB;
            dest.User = source.User;
            dest.Pwd = source.Pwd;
            dest.CharSet = source.CharSet;
            dest.Encrypt = source.Encrypt;
            dest.Compress = source.Compress;
        }

        public void Execute(Instance ins, IStopStatus status)
        {
            this.status = status;

            foreach (Common.Task t in ins.Tasks)
            {
                if (status.IsStopped()) break;

                if (t is MigrationTask task && task.Tables.Length > 0)
                {
                    task.StartTick = WinAPI.GetTickCount();
                    task.Status = DataStates.Running;

                    // 构建待迁移表清单：lst[0] = 独立表，lst[1+] = 依赖树
                    List<List<MigrationTable>> lst = new List<List<MigrationTable>>
                    {
                        new List<MigrationTable>(task.Tables[0]),
                        new List<MigrationTable>()
                    };

                    for (int i = 1; i < task.Tables.Length; i++)
                    {
                        for (int j = 0; j < task.Tables[i].Length; j++)
                        {
                            MigrationTable ta = task.Tables[i][j];

                            for (int k = 0; k < ta.References.Length; k++)
                                for (int l = 0; l < lst[0].Count; l++)
                                    if (ta.References[k].Equals(lst[0][l].DestName))
                                    {
                                        lst[1].Add(lst[0][l]);
                                        lst[0].RemoveAt(l);
                                        break;
                                    }
                        }
                        lst.Add(new List<MigrationTable>(task.Tables[i]));
                    }

                    TableComparer comparer = new TableComparer();

                    foreach (List<MigrationTable> tables in lst)
                        tables.Sort(comparer);

                    List<MigrationTable> runList = new List<MigrationTable>();

                    // 开始迁移
                    try
                    {
                        Parallel.ForEach(CreateThreadAction((int)task.Threads), i =>
                        {
                            MigrationTable table = GetTable(lst, runList);

                            while (table != null)
                            {
                                Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.DestName}", "迁移开始...");

                                lock (runList) { runList.Add(table); }
                                MigrateTable(task, table, out string reason);
                                if (table.Status == DataStates.Done)
                                {
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.DestName}", "迁移成功。");
                                    Logger.WriteRpt(task.Dest.Server, task.Dest.DB, table.DestName, "成功", reason);
                                }
                                else
                                {
                                    task.Status = DataStates.RunningError;
                                    task.Progress -= table.Progress;
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{table.DestName}",
                                        $"迁移失败！{reason}");
                                    Logger.WriteRpt(task.Dest.Server, task.Dest.DB, table.DestName, "失败", reason);
                                }
                                lock (runList) { runList.Remove(table); }

                                table = GetTable(lst, runList);
                            }
                        });

                        if (status.IsStopped() || task.Status == DataStates.RunningError || task.Status == DataStates.Error)
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

        private MigrationTable GetTable(List<List<MigrationTable>> lst, List<MigrationTable> runList)
        {
            while (true)
            {
                lock (lst)
                {
                    int dependCount = 0;
                    // 先从依赖树取
                    for (int i = 1; i < lst.Count; i++)
                    {
                        dependCount += lst[i].Count;
                        for (int j = 0; j < lst[i].Count; j++)
                        {
                            // 无外键依赖
                            if (lst[i][j].References.Length == 0)
                            {
                                MigrationTable rst = lst[i][j];

                                lst[i].RemoveAt(j);

                                return rst;
                            }
                            else
                            {
                                bool inTree = false;

                                foreach (string s in lst[i][j].References)
                                {
                                    for (int k = 1; k < i; k++)
                                    {
                                        for (int l = 0; l < lst[k].Count; l++)
                                            if (s.Equals(lst[k][l].DestName))
                                            {
                                                inTree = true;
                                                break;
                                            }
                                        if (inTree) break;
                                    }
                                    if (inTree) break;

                                    lock (runList)
                                    {
                                        for (int k = 0; k < runList.Count; k++)
                                            if (s.Equals(runList[k].DestName))
                                            {
                                                inTree = true;
                                                break;
                                            }
                                    }
                                }

                                if (!inTree)
                                {
                                    MigrationTable rst = lst[i][j];

                                    lst[i].RemoveAt(j);

                                    return rst;
                                }
                            }
                        }
                    }

                    // 再从独立表取
                    if (lst[0].Count > 0)
                    {
                        MigrationTable rst = lst[0][0];

                        lst[0].RemoveAt(0);

                        return rst;
                    }
                    else if (dependCount == 0)
                        return null;

                    Thread.Sleep(50);
                }
            }
        }

        private JObject LoadAndDeserialize(string file)
        {
            return JsonConvert.DeserializeObject(File.ReadAllText(file)) as JObject;
        }

        public void LoadSample(Instance ins, Database source, Database dest, List<Table> tables, out string param)
        {
            if (ins.Tasks[0] is MigrationTask task)
            {
                DuplicateDatabase(task.Source, source);
                DuplicateDatabase(task.Dest, dest);

                foreach (Table[] t in task.Tables)
                    tables.AddRange(t);

                param = task.Params;
            }
            else
                param = "";
        }

        private void MigrateTable(MigrationTask task, MigrationTable table, out string reason)
        {
            reason = "取消操作";
            if (status.IsStopped()) return;

            if (Connect(task.Source, out IDBMSReader source) && Connect(task.Dest, out IDBMSWriter dest))
            {
                Dictionary<string, object> parms = new Dictionary<string, object>();

                dest.QueryParam(task.Params, parms);
                dest.BeginTransaction();
                try
                {
                    // 迁移数据
                    MigrateTableWithScript(task, table, parms, source, dest, out reason);

                    // 迁移后校验
                    if (table.Status != DataStates.Error && !status.IsStopped())
                        SummateTable(table, parms, source, dest, out reason);

                    if (table.Status != DataStates.Error && !status.IsStopped())
                    {
                        dest.CommitTransaction();
                        table.Status = DataStates.Done;
                    }
                    else
                    {
                        dest.RollbackTransaction();
                        table.Status = DataStates.Error;
                    }
                }
                catch (Exception ex)
                {
                    dest.RollbackTransaction();
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
                            while (scripts.Count > bufSize && !status.IsStopped() && table.Status != DataStates.Error)
                                Thread.Sleep(50);

                            if (status.IsStopped() || table.Status == DataStates.Error) break;

                            if (source.QueryPage(table, fromRow, toRow, WithEnums.NoLock, parms, out data))
                                try
                                {
                                    object script = null;

                                    data.MapFields(table.DestFields);
                                    while (dest.BuildScript(table, data, filter, out script)
                                        && !status.IsStopped() && table.Status != DataStates.Error)
                                        scripts.Enqueue(script);

                                    // 获取不到预期的记录数，作最后一页处理
                                    if (data.ReadCount != task.ReadPages * table.PageSize || status.IsStopped()) break;
                                }
                                finally
                                {
                                    data.Close();
                                }
                            else
                            {
                                table.Status = DataStates.Error;
                                reason = source.GetLastError();
                                break;
                            }

                            fromRow = toRow + 1;
                            toRow += task.ReadPages * table.PageSize;
                        }

                        read = true;
                    }
                    else if ("write".Equals(act))
                    {
                        while (table.Status != DataStates.Error && (!read || scripts.Count > 0) && !status.IsStopped())
                            if (scripts.Count > 0)
                            {
                                scripts.TryDequeue(out object script);
                                if (!dest.ExecScript(table, script, out uint r))
                                {
                                    table.Status = DataStates.Error;
                                    reason = dest.GetLastError();
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
                if (status.IsStopped()) break;

                if (t is MigrationTask task)
                {
                    task.Progress = 0;
                    task.Total = 0;
                    task.Status = DataStates.Running;
                    if (Connect(task.Source, out IDBMSReader source) && Connect(task.Dest, out IDBMSWriter dest))
                    {
                        Dictionary<string, object> parms = new Dictionary<string, object>();

                        dest.QueryParam(task.Params, parms);

                        for (int i = 0; i < task.Tables.Length; i++)
                            for (int j = 0; j < task.Tables[i].Length; j++)
                                if (!status.IsStopped())
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
                    if (source.GetFieldNames(table.SourceName, out string[] fields))
                    {
                        table.SourceFields = fields;

                        // #2: 获取待迁移记录数
                        if (source.QueryCount(table.SourceName, table.SourceWhereSQL, WithEnums.NoLock, parms,
                            out ulong count))
                        {
                            table.Progress = 0;
                            task.Total += count;
                            table.Total = count;
                            isError = false;
                        }
                    }

                    if (isError) table.Status = DataStates.Error;
                }
                else if ("write".Equals(act))
                {
                    if (dest.GetFieldNames(table.DestName, out string[] fields))
                        table.DestFields = fields;
                    else
                        table.Status = DataStates.Error;
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
                    ["name"] = t.SourceName.Equals(t.DestName) ? t.SourceName : $"{t.SourceName},{t.DestName}",
                    ["order"] = t.Order,
                    ["orderSQL"] = t.OrderSQL,
                    ["whereSQL"] = t.SourceWhereSQL,
                    ["pageSize"] = t.PageSize,
                    ["mode"] = t.WriteMode == WriteModes.Append ? "Append" : "Update",
                    ["keyFields"] = t.KeyFields == null ? "" : string.Join(",", t.KeyFields),
                    ["skipFields"] = t.SkipFields == null ? "" : string.Join(",", t.SkipFields),
                    ["filter"] = t.Filter,
                    ["References"] = t.References == null ? "" : string.Join(",", t.References)
                };

                if (!t.SourceWhereSQL.Equals(t.DestWhereSQL))
                    obj["destWhereSQL"] = t.DestWhereSQL;

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
                ["user"] = source.User,
                ["password"] = source.Pwd,
                ["charset"] = source.CharSet,
                ["compress"] = source.Compress ? 1 : 0,
                ["encrypt"] = source.Encrypt ? 1 : 0
            };
            JObject dstDB = new JObject()
            {
                ["dbms"] = dest.DBMS,
                ["server"] = dest.Server,
                ["port"] = dest.Port,
                ["user"] = dest.User,
                ["password"] = dest.Pwd,
                ["charset"] = dest.CharSet,
                ["compress"] = dest.Compress ? 1 : 0,
                ["encrypt"] = dest.Encrypt ? 1 : 0
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

        private void SummateTable(MigrationTable table, Dictionary<string, object> parms, IDBMSReader source,
            IDBMSWriter dest, out string failReason)
        {
            ulong sourceCount = 0, destCount = 0;
            string reason = "";

            Parallel.ForEach(CreateThreadAction(), act =>
            {
                if ("read".Equals(act))
                {
                    if (!source.QueryCount(table.SourceName, table.SourceWhereSQL, WithEnums.NoLock, parms,
                        out sourceCount))
                    {
                        table.Status = DataStates.Error;
                        reason = source.GetLastError();
                    }
                }
                else if ("write".Equals(act))
                {
                    if (!dest.QueryCount(table.DestName, table.DestWhereSQL, WithEnums.NoLock, parms,
                        out destCount))
                    {
                        table.Status = DataStates.Error;
                        reason = dest.GetLastError();
                    }
                }
            });

            if (table.Status == DataStates.Error) failReason = reason;
            else if (sourceCount != destCount)
            {
                table.Status = DataStates.Error;
                failReason = $"{table.DestName} 数据校验失败！源数量：{sourceCount:#,##0}，目标数量：{destCount:#,##0}。";
            }
            else
                failReason = sourceCount.ToString("#,##0");
        }

        private void WriteFile(string file, string content)
        {
            try
            {
                using (FileStream fs = new FileStream(file, FileMode.Create))
                {
                    using (StreamWriter writer = new StreamWriter(fs, new UTF8Encoding(false)))
                    {
                        writer.Write(content);
                        writer.Flush();
                    }
                }
            }
            catch (Exception) { }
        }
    }
}

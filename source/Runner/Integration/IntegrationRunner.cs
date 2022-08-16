using JHWork.DataMigration.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// IntegrationRunner 的实例结构如下：
//
// IntegrationInstance
//   + IntegrationTask[][] (并行)
//      + IntegrationTable
//
// 即，每个实例包含一组串行任务（二维数组结构用于解决表之间的依赖关系），每个任务可包含一组并行汇集表

namespace JHWork.DataMigration.Runner.Integration
{
    /// <summary>
    /// 汇集执行器
    /// </summary>
    public class IntegrationRunner : RunnerBase, IAssemblyLoader, IRunnerAnalyzer, IRunnerExecutor, IRunnerAssistant
    {
        private IStopStatus status;

        private Database AnalyseDatabase(JObject obj, JObject inherited)
        {
            return new Database()
            {
                DBMS = GetJValue(obj, inherited, "dbms", "dest"),
                Server = GetJValue(obj, inherited, "server", "dest"),
                Port = uint.Parse(GetJValue(obj, inherited, "port", "dest")),
                DB = GetJValue(obj, inherited, "db", "dest"),
                Schema = GetJValue(obj, inherited, "schema", "dest"),
                User = GetJValue(obj, inherited, "user", "dest"),
                Pwd = GetJValue(obj, inherited, "password", "dest"),
                CharSet = GetJValue(obj, inherited, "charset", "dest", "utf8"),
                Encrypt = int.Parse(GetJValue(obj, inherited, "encrypt", "dest", "0")) != 0,
                Compress = int.Parse(GetJValue(obj, inherited, "compress", "dest", "0")) != 0,
                Timeout = uint.Parse(GetJValue(obj, inherited, "timeout", "dest", "60"))
            };
        }

        private Database[] AnalyseDatabases(JArray objs, JObject inherited)
        {
            List<Database> lst = new List<Database>();

            foreach (JObject obj in objs)
            {
                JArray dbs = obj["dbs"] as JArray;

                foreach (JToken db in dbs)
                {
                    string schema = Database.AnalyseSchema(db.ToString());

                    lst.Add(new Database()
                    {
                        DBMS = GetJValue(obj, inherited, "dbms", "source"),
                        Server = GetJValue(obj, inherited, "server", "source"),
                        Port = uint.Parse(GetJValue(obj, inherited, "port", "source")),
                        DB = Database.AnalyseDB(db.ToString()),
                        Schema = string.IsNullOrEmpty(schema) ? GetJValue(obj, inherited, "schema", "source") : schema,
                        User = GetJValue(obj, inherited, "user", "source"),
                        Pwd = GetJValue(obj, inherited, "password", "source"),
                        CharSet = GetJValue(obj, inherited, "charset", "source", "utf8"),
                        Encrypt = int.Parse(GetJValue(obj, inherited, "encrypt", "source", "0")) != 0,
                        Compress = int.Parse(GetJValue(obj, inherited, "compress", "source", "0")) != 0,
                        Timeout = uint.Parse(GetJValue(obj, inherited, "timeout", "source", "60"))
                    });
                }
            }

            return lst.ToArray();
        }

        public Instance[] AnalyseInstance(JArray objs, JObject inherited, string path)
        {
            List<IntegrationInstance> instances = new List<IntegrationInstance>();

            foreach (JObject obj in objs)
            {
                IntegrationTable[][] tables = AnalyseTable($"{path}\\{GetJValue(obj, inherited, "tables")}",
                    out string parms);
                IntegrationTask[][] tasks = new IntegrationTask[tables.Length][];
                List<IntegrationTask> taskList = new List<IntegrationTask>();

                for (int i = 0; i < tasks.Length; i++)
                {
                    if (tables[i].Length > 0)
                    {
                        tasks[i] = new IntegrationTask[tables[i].Length];
                        for (int j = 0; j < tasks[i].Length; j++)
                        {
                            IntegrationTask task = new IntegrationTask()
                            {
                                Name = tables[i][j].DestName,
                                Progress = 0,
                                Total = 0,
                                Status = DataStates.Idle,
                                StartTick = 0,
                                Sources = AnalyseDatabases(obj["sources"] as JArray, inherited),
                                Dest = AnalyseDatabase(obj["dest"] as JObject, inherited),
                                Params = parms,
                                Table = tables[i][j],
                                ReadPages = uint.Parse(GetJValue(obj, inherited, "readPages"))
                            };

                            taskList.Add(task);
                            tasks[i][j] = task;
                        }
                    }
                }

                if (taskList.Count > 0)
                {
                    IntegrationInstance ins = new IntegrationInstance()
                    {
                        Name = $"{taskList[0].Dest.Server}:{taskList[0].Dest.Port}/{taskList[0].Dest.DB}",
                        Tasks = taskList.ToArray(),
                        ActualTasks = tasks,
                        Threads = uint.Parse(GetJValue(obj, inherited, "threads"))
                    };

                    instances.Add(ins);
                }
            }

            return instances.ToArray();
        }

        private IntegrationTable[][] AnalyseTable(string file, out string parms)
        {
            JObject obj = LoadAndDeserialize(file);
            JArray tables = obj["tables"] as JArray;
            List<IntegrationTable> buf = new List<IntegrationTable>();

            parms = obj["params"].ToString();

            // #1: 获取所有表
            for (int i = 0; i < tables.Count; i++)
            {
                JObject o = tables[i] as JObject;
                string[] names = o["name"].ToString().Split(',');
                IntegrationTable table = new IntegrationTable()
                {
                    SourceName = Table.AnalyseName(names[0]),
                    SourceSchema = Table.AnalyseSchema(names[0]),
                    DestName = Table.AnalyseName(names.Length > 1 ? names[1] : names[0]),
                    DestSchema = Table.AnalyseSchema(names.Length > 1 ? names[1] : names[0]),
                    Order = int.Parse(o["order"].ToString()),
                    OrderSQL = o["orderSQL"].ToString(),
                    WhereSQL = o["whereSQL"].ToString(),
                    PageSize = uint.Parse(o["pageSize"].ToString()),
                    WriteMode = "UPDATE".Equals(o["mode"].ToString().ToUpper()) ? WriteModes.Update
                        : WriteModes.Append,
                    KeyFields = o["keyFields"].ToString().Length > 0 ? o["keyFields"].ToString().Split(',') : new string[0],
                    SkipFields = o["skipFields"].ToString().Split(','),
                    Filter = o["filter"].ToString(),
                    KeepIdentity = false,
                    DestFields = new string[] { },
                    References = o.ContainsKey("references") ? o["references"].ToString().Split(',') : new string[] { },
                    Total = 0,
                    Progress = 0
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
                List<IntegrationTable> tmpBuf = new List<IntegrationTable>() { buf[0] };
                List<List<IntegrationTable>> rstBuf = new List<List<IntegrationTable>>() { tmpBuf };

                for (int i = 1; i < buf.Count; i++)
                {
                    if (buf[i].Order != order)
                    {
                        tmpBuf = new List<IntegrationTable>() { buf[i] };
                        rstBuf.Add(tmpBuf);
                        order = buf[i].Order;
                    }
                    else
                        tmpBuf.Add(buf[i]);
                }

                IntegrationTable[][] rst = new IntegrationTable[rstBuf.Count][];
                for (int i = 0; i < rstBuf.Count; i++)
                    rst[i] = rstBuf[i].ToArray();

                return rst;
            }
            else
                return new IntegrationTable[][] { };
        }

        public void Execute(Instance ins, IStopStatus status, bool withTrans)
        {
            this.status = status;

            if (ins is IntegrationInstance instance && instance.ActualTasks.Length > 0)
            {
                // 构建待汇集任务清单
                List<List<IntegrationTask>> lst = new List<List<IntegrationTask>>();
                TaskComparer comparer = new TaskComparer();

                for (int i = 0; i < instance.ActualTasks.Length; i++)
                {
                    List<IntegrationTask> tasks = new List<IntegrationTask>(instance.ActualTasks[i]);

                    tasks.Sort(comparer);
                    lst.Add(tasks);
                }

                // 开始汇集
                foreach (List<IntegrationTask> tasks in lst)
                    Parallel.ForEach(CreateThreadAction((int)instance.Threads), _ =>
                    {
                        IntegrationTask task = GetTask(tasks);

                        while (task != null)
                        {
                            try
                            {
                                Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{task.Table.DestName}", "汇集开始...");

                                task.StartTick = WinAPI.GetTickCount();
                                task.Status = DataStates.Running;

                                IntegrateTask(task, withTrans, out string reason);
                                if (task.Status == DataStates.Done)
                                {
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{task.Table.DestName}", "汇集成功。");
                                    Logger.WriteRpt(task.Dest.Server, task.Dest.DB, task.Table.DestName, "成功",
                                            task.Table.Progress.ToString("#,##0"));
                                }
                                else
                                {
                                    task.Progress -= task.Table.Progress;
                                    task.ErrorMsg = reason;
                                    Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}.{task.Table.DestName}",
                                            $"汇集失败！{reason}");
                                    Logger.WriteRpt(task.Dest.Server, task.Dest.DB, task.Table.DestName, "失败", reason);
                                }
                                task = GetTask(tasks);
                            }
                            catch (Exception ex)
                            {
                                task.Status = DataStates.Error;
                                task.ErrorMsg = ex.Message;
                                Logger.WriteLog($"{task.Dest.Server}/{task.Dest.DB}", $"汇集失败！{ex.Message}");
                            }
                        }
                    });

                foreach (Common.Task task in instance.Tasks)
                    if (task.Status != DataStates.Done)
                    {
                        Logger.WriteLog(instance.Name, "汇集失败！");
                        return;
                    }

                Logger.WriteLog(instance.Name, "汇集成功。");
            }
        }

        public string GetName()
        {
            return "Integration";
        }

        private IntegrationTask GetTask(List<IntegrationTask> tasks)
        {
            lock (tasks)
            {
                if (tasks.Count > 0)
                {
                    IntegrationTask task = tasks[0];

                    tasks.RemoveAt(0);

                    return task;
                }
                else
                    return null;
            }
        }

        private void IntegrateTask(IntegrationTask task, bool withTrans, out string reason)
        {
            reason = "取消操作";
            if (status.Stopped) return;

            if (Connect(task, null, out _, task.Dest, out IDBMSWriter dest))
            {
                Dictionary<string, object> parms = new Dictionary<string, object>();

                dest.QueryParam(task.Params, parms);
                if (withTrans) dest.BeginTransaction();
                try
                {
                    // 汇集数据
                    IntegrateTaskWithScript(task, parms, dest, out reason);

                    if (task.Status == DataStates.Done)
                    {
                        if (withTrans) dest.CommitTransaction();
                    }
                    else
                        if (withTrans) dest.RollbackTransaction();
                }
                catch (Exception ex)
                {
                    if (withTrans) dest.RollbackTransaction();
                    task.Status = DataStates.Error;
                    reason = ex.Message;
                }

                dest.Close();
            }
            else
            {
                task.Status = DataStates.Error;
                reason = task.ErrorMsg;
            }
        }

        private void IntegrateTaskWithScript(IntegrationTask task, Dictionary<string, object> parms, IDBMSWriter dest,
            out string failReason)
        {
            ConcurrentQueue<object> scripts = new ConcurrentQueue<object>();
            bool read = false;
            string reason = "取消操作";

            Parallel.ForEach(CreateThreadAction(), act =>
            {
                try
                {
                    // “读”线程：个别数据源读取失败，容错继续执行
                    if ("read".Equals(act))
                    {
                        uint bufSize = task.ReadPages * 3;
                        IDataFilter filter = DataFilterFactory.GetFilterByName(task.Table.Filter);
                        IDataWrapper data = null;

                        foreach (Database db in task.Sources)
                        {
                            if (status.Stopped || task.Status == DataStates.Error) break;

                            uint fromRow = 1, toRow = task.ReadPages * task.Table.PageSize;
                            Dictionary<string, object> tmpParams = new Dictionary<string, object>(parms);

                            // 连接数据源
                            if (!Connect(task, db, out IDBMSReader source, null, out _))
                            {
                                task.Status = DataStates.RunningError;
                                reason = task.ErrorMsg;
                                break;
                            }

                            while (true)
                            {
                                // 等待缓冲区可用
                                while (scripts.Count > bufSize && !status.Stopped && task.Status != DataStates.Error)
                                    Thread.Sleep(50);

                                if (status.Stopped || task.Status == DataStates.Error) break;

                                // 取数
                                if (source.QueryPage(task.Table, fromRow, toRow, WithEnums.NoLock, tmpParams, out data))
                                    try
                                    {
                                        object script = null;

                                        data.MapFields(task.Table.DestFields);
                                        while (dest.BuildScript(task.Table, data, filter, out script)
                                            && !status.Stopped && task.Status != DataStates.Error)
                                            scripts.Enqueue(script);

                                        // 获取不到预期的记录数，作最后一页处理
                                        if (data.ReadCount != task.ReadPages * task.Table.PageSize
                                            || status.Stopped) break;
                                    }
                                    finally
                                    {
                                        data.Close();
                                    }
                                else
                                {
                                    task.Status = DataStates.RunningError;
                                    reason = source.LastError;
                                    break;
                                }

                                fromRow = toRow + 1;
                                toRow += task.ReadPages * task.Table.PageSize;
                            }

                            source.Close();
                        }
                        read = true;
                    }
                    // “写”线程：写失败则直接停止执行
                    else if ("write".Equals(act))
                    {
                        while (task.Status != DataStates.Error && (!read || scripts.Count > 0) && !status.Stopped)
                            if (scripts.Count > 0)
                            {
                                scripts.TryDequeue(out object script);
                                if (!dest.ExecScript(task.Table, script, out uint r))
                                {
                                    task.Status = DataStates.Error;
                                    reason = dest.LastError;
                                    break;
                                }

                                lock (task)
                                {
                                    task.Table.Progress += r;
                                    task.Progress += r;
                                }
                            }
                            else
                                Thread.Sleep(1);
                    }
                }
                catch (Exception ex)
                {
                    task.Status = DataStates.Error;
                    reason = ex.Message;
                }
            });

            if (task.Status == DataStates.Error || task.Status == DataStates.RunningError || status.Stopped)
            {
                task.Status = DataStates.Error;
                failReason = reason;
            }
            else
            {
                task.Status = DataStates.Done;
                failReason = "";
            }
        }

        public void LoadSample(Instance ins, Database source, Database dest, List<Table> tables, out string param)
        {
            if (ins.Tasks[0] is IntegrationTask task)
            {
                source.Duplicate(task.Sources[0]);
                dest.Duplicate(task.Dest);
                param = task.Params;
            }
            else
                param = "";

            foreach (Common.Task t in ins.Tasks)
                if (t is IntegrationTask ta) tables.Add(ta.Table);
        }

        public void Prefetch(Instance ins, IStopStatus status)
        {
            this.status = status;

            foreach (Common.Task t in ins.Tasks)
            {
                if (status.Stopped) break;

                if (t is IntegrationTask task)
                {
                    task.Progress = 0;
                    task.Total = 0;
                    task.Status = DataStates.Running;
                    task.Table.Total = 0;
                    task.Table.Progress = 0;

                    if (Connect(task, null, out _, task.Dest, out IDBMSWriter dest))
                    {
                        Dictionary<string, object> parms = new Dictionary<string, object>();

                        if (!dest.QueryParam(task.Params, parms))
                        {
                            task.ErrorMsg = dest.LastError;
                            break;
                        }

                        Parallel.ForEach(CreateThreadAction(), act =>
                        {
                            if ("read".Equals(act))
                            {
                                IDBMSReader source = null;

                                foreach (Database db in task.Sources)
                                {
                                    if (task.Status == DataStates.Error || status.Stopped) break;

                                    if (Connect(task, db, out source, null, out _))
                                    {
                                        bool isError = true;

                                        // #1: 检查表存在，并缓存字段
                                        if (source.GetFieldNames(task.Table.SourceName, task.Table.SourceSchema, out string[] fields))
                                        {
                                            task.Table.SourceFields = fields;

                                            // #2: 获取记录数
                                            if (source.QueryCount(task.Table, WithEnums.NoLock, parms, out ulong count))
                                            {
                                                task.Total += count;
                                                task.Table.Total += count;
                                                isError = false;
                                            }
                                        }

                                        if (isError)
                                        {
                                            task.Status = DataStates.Error;
                                            task.ErrorMsg = source.LastError;
                                        }
                                        source.Close();
                                    }
                                    else
                                        task.Status = DataStates.Error;
                                }
                            }
                            else if ("write".Equals(act))
                            {
                                if (dest.GetFieldNames(task.Table.DestName, task.Table.DestSchema, out string[] fields))
                                    task.Table.DestFields = fields;
                                else
                                {
                                    task.Status = DataStates.Error;
                                    task.ErrorMsg = dest.LastError;
                                }
                                dest.Close();
                            }
                        });
                        if (task.Status != DataStates.Error) task.Status = DataStates.Idle;
                    }
                    else
                        task.Status = DataStates.Error;
                }
            }
        }

        public void Reset(Instance ins)
        {
            foreach (Common.Task t in ins.Tasks)
                if (t is IntegrationTask task)
                {
                    task.Progress = 0;
                    task.StartTick = 0;
                    task.Status = DataStates.Idle;
                    task.Total = 0;
                    task.Table.Total = 0;
                    task.Table.Progress = 0;
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
                ["db"] = source.DB,
                ["schema"] = source.Schema,
                ["user"] = source.User,
                ["password"] = source.Pwd,
                ["charset"] = source.CharSet,
                ["encrypt"] = source.Encrypt,
                ["compress"] = source.Compress,
                ["timeout"] = source.Timeout
            };
            JArray sourceArray = new JArray() { srcDB };
            JObject dstDB = new JObject()
            {
                ["dbms"] = dest.DBMS,
                ["server"] = dest.Server,
                ["port"] = dest.Port,
                ["db"] = dest.DB,
                ["schema"] = dest.Schema,
                ["user"] = dest.User,
                ["password"] = dest.Pwd,
                ["charset"] = dest.CharSet,
                ["encrypt"] = dest.Encrypt,
                ["compress"] = dest.Compress,
                ["timeout"] = dest.Timeout
            };
            JObject instance = new JObject()
            {
                ["sources"] = sourceArray,
                ["dest"] = dstDB,
                ["tables"] = $"Table-{file}",
                ["readPages"] = 10,
                ["threads"] = 1
            };
            JArray insArray = new JArray() { instance };

            JObject profileData = new JObject()
            {
                ["instances"] = insArray,
                ["mode"] = "Once",
                ["runner"] = "Integration",
                ["runtime"] = "00:00",
                ["threads"] = 1
            };

            WriteFile($"{path}Profile-{file}", JsonConvert.SerializeObject(profileData, Formatting.Indented));
        }
    }

    /// <summary>
    /// 汇集任务排序对比类
    /// </summary>
    internal class TaskComparer : IComparer<IntegrationTask>
    {
        private readonly TableComparer comparer = new TableComparer();

        /// <summary>
        /// 从小到大排序比对
        /// </summary>
        /// <param name="x">汇集任务</param>
        /// <param name="y">汇集任务</param>
        /// <returns>从小到大排序比对结果</returns>
        public int Compare(IntegrationTask x, IntegrationTask y)
        {
            return comparer.Compare(x.Table, y.Table);
        }
    }
}

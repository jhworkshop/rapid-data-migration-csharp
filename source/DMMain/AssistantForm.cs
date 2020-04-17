using JHWork.DataMigration.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Windows.Forms;

namespace JHWork.DataMigration
{
    /// <summary>
    /// 配置助手
    /// </summary>
    public partial class AssistantForm : Form, IProgress
    {
        private string importedParam = "";
        private List<Table> importedTables = new List<Table>();

        public AssistantForm()
        {
            InitializeComponent();
        }

        private void ButtonImport_Click(object sender, EventArgs e)
        {
            if (openDialog.ShowDialog() == DialogResult.OK)
            {
                Profile profile = new Profile();
                try
                {
                    profile.Load(openDialog.FileName);

                    IRunnerAssistant assist = RunnerFactory.GetRunnerAssistantByName(profile.Runner);

                    if (assist == null) throw new Exception($"执行器 {profile.Runner} 不支持！");

                    Database source = new Database(), dest = new Database();

                    importedTables.Clear();

                    // 暂时只支持第一个实例
                    assist.LoadSample(profile.Instances[0], source, dest, importedTables, out importedParam);

                    runner.Text = profile.Runner;

                    sourceDBMS.Text = source.DBMS;
                    sourceServer.Text = source.Server;
                    sourcePort.Text = source.Port.ToString();
                    sourceDB.Text = source.DB;
                    sourceUser.Text = source.User;
                    sourcePwd.Text = source.Pwd;
                    sourceCharSet.Text = source.CharSet;

                    destDBMS.Text = dest.DBMS;
                    destServer.Text = dest.Server;
                    destPort.Text = dest.Port.ToString();
                    destDB.Text = dest.DB;
                    destUser.Text = dest.User;
                    destPwd.Text = dest.Pwd;
                    destCharSet.Text = dest.CharSet;

                    RetrieveTables();
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"配置文件有误！{ex.Message}", "加载配置", MessageBoxButtons.OK);
                }
            }
        }

        private void ButtonRetrieve_Click(object sender, EventArgs e)
        {
            RetrieveTables();
        }

        private void ButtonReverse_Click(object sener, EventArgs e)
        {
            foreach (ListViewItem item in listView.Items)
                item.Checked = !item.Checked;
        }

        private void ButtonSave_Click(object sender, EventArgs e)
        {
            if (saveDialog.ShowDialog() == DialogResult.OK)
            {
                IRunnerAssistant assist = RunnerFactory.GetRunnerAssistantByName(runner.Text);
                string path = Path.GetDirectoryName(saveDialog.FileName) + "\\";
                string file = Path.GetFileName(saveDialog.FileName);
                Database source = new Database()
                {
                    DBMS = sourceDBMS.Text,
                    Server = sourceServer.Text,
                    Port = uint.Parse(sourcePort.Text),
                    DB = sourceDB.Text,
                    User = sourceUser.Text,
                    Pwd = sourcePwd.Text,
                    CharSet = sourceCharSet.Text
                };
                Database dest = new Database()
                {
                    DBMS = destDBMS.Text,
                    Server = destServer.Text,
                    Port = uint.Parse(destPort.Text),
                    DB = destDB.Text,
                    User = destUser.Text,
                    Pwd = destPwd.Text,
                    CharSet = destCharSet.Text
                };
                List<Table> tables = new List<Table>();

                for (int i = 0; i < listView.Items.Count; i++)
                {
                    ListViewItem item = listView.Items[i];

                    if (item.Checked && item.Tag is Table table)
                        tables.Add(table);
                }

                assist.SaveSample(source, dest, tables, importedParam, path, file);
            }
        }

        private void ButtonSelectAll_Click(object sender, EventArgs e)
        {
            foreach (ListViewItem item in listView.Items)
                item.Checked = true;
        }

        private void ProfileForm_Load(object sender, EventArgs e)
        {
            sourceDBMS.Items.AddRange(DBMSFactory.GetDBMSReaderNames());
            destDBMS.Items.AddRange(DBMSFactory.GetDBMSWriterNames());
            runner.Items.AddRange(RunnerFactory.GetRunnerAssistantNames());

            string[] charsets = new string[] { "gbk", "utf8" };

            sourceCharSet.Items.AddRange(charsets);
            destCharSet.Items.AddRange(charsets);
        }

        private bool InitDataSource(ref IDBMSAssistant source, ref IDBMSAssistant dest)
        {
            source = DBMSFactory.GetDBMSAssistantByName(sourceDBMS.Text);
            dest = DBMSFactory.GetDBMSAssistantByName(destDBMS.Text);
            if (source != null && dest != null)
            {
                Database srcDB = new Database()
                {
                    DBMS = sourceDBMS.Text,
                    Server = sourceServer.Text,
                    Port = uint.Parse(sourcePort.Text),
                    DB = sourceDB.Text,
                    User = sourceUser.Text,
                    Pwd = sourcePwd.Text,
                    CharSet = sourceCharSet.Text
                };
                Database dstDB = new Database()
                {
                    DBMS = destDBMS.Text,
                    Server = destServer.Text,
                    Port = uint.Parse(destPort.Text),
                    DB = destDB.Text,
                    User = destUser.Text,
                    Pwd = destPwd.Text,
                    CharSet = destCharSet.Text
                };

                return source.Connect(srcDB) && dest.Connect(dstDB);
            }

            return false;
        }

        private void RetrieveTables()
        {
            progressLabel.Visible = true;
            progressBar.Value = 0;
            progressBar.Maximum = 100;
            progressBar.Visible = true;
            Application.DoEvents();

            IDBMSAssistant source = null, dest = null;

            if (!InitDataSource(ref source, ref dest))
                MessageBox.Show("数据源配置有误！", "配置助手", MessageBoxButtons.OK);
            else
            {
                List<TableInfo> sourceInfo = new List<TableInfo>(), destInfo = new List<TableInfo>();

                source.GetTables(this, sourceInfo);
                dest.GetTables(this, destInfo);

                listView.Items.Clear();

                while (destInfo.Count > 0)
                {
                    TableInfo info = destInfo[0];

                    destInfo.RemoveAt(0);

                    Table table = new Table()
                    {
                        SourceName = "<?>",
                        DestName = info.Name,
                        Order = info.Order,
                        PageSize = 100,
                        OrderSQL = info.KeyFields.Length == 0 ? "" : string.Join(" ASC, ", info.KeyFields) + " ASC",
                        SourceWhereSQL = "",
                        DestWhereSQL = "",
                        WriteMode = WriteModes.Append,
                        KeyFields = info.KeyFields,
                        SkipFields = { },
                        Filter = ""
                    };
                    string sourceName = "";
                    bool found = false;

                    // 区分大小写匹配引入配置目标表
                    for (int i = 0; i < importedTables.Count; i++)
                        if (table.DestName.Equals(importedTables[i].DestName))
                        {
                            sourceName = importedTables[i].SourceName;
                            table.PageSize = importedTables[i].PageSize;
                            table.OrderSQL = importedTables[i].OrderSQL;
                            table.SourceWhereSQL = importedTables[i].SourceWhereSQL;
                            table.DestWhereSQL = importedTables[i].DestWhereSQL;
                            table.WriteMode = importedTables[i].WriteMode;
                            table.SkipFields = importedTables[i].SkipFields;
                            table.Filter = importedTables[i].Filter;
                            found = true;

                            break;
                        }

                    // 不区分大小写匹配源数据表
                    if (found)
                        sourceName = sourceName.ToLower();
                    else
                        sourceName = table.DestName.ToLower();

                    for (int i = 0; i < sourceInfo.Count; i++)
                        if (sourceName.Equals(sourceInfo[i].Name.ToLower()))
                        {
                            table.SourceName = sourceInfo[i].Name;
                            sourceInfo.RemoveAt(i);
                            break;
                        }

                    ListViewItem item = listView.Items.Add(table.SourceName);

                    item.SubItems.Add(table.DestName);
                    item.SubItems.Add(table.Order.ToString());
                    item.Checked = found;
                    item.Tag = table;
                }
            }
            progressLabel.Visible = false;
            progressBar.Visible = false;
        }

        public void OnProgress(int total, int progress)
        {
            progressBar.Maximum = total;
            progressBar.Value = progress;
            Application.DoEvents();
        }
    }
}

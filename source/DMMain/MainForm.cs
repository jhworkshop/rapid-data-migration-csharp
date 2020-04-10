using JHWork.DataMigration.Common;
using System;
using System.Threading;
using System.Windows.Forms;

namespace JHWork.DataMigration
{
    /// <summary>
    /// 锁定类型
    /// </summary>
    enum LockTypes
    {
        Ready,   // 就绪
        Loading, // 正在加载
        Running  // 正在执行
    }

    /// <summary>
    /// 主窗口
    /// </summary>
    public partial class MainForm : Form
    {
        private readonly Profile profile = new Profile();
        private readonly Executor executor = new Executor();

        public MainForm()
        {
            InitializeComponent();
        }

        private void Lock(LockTypes type)
        {
            switch (type)
            {
                case LockTypes.Ready:
                    btnLoad.Enabled = true;
                    btnRun.Visible = true;
                    btnRun.Enabled = listView.Items.Count > 0;
                    btnStop.Visible = false;
                    btnProfile.Enabled = true;
                    statusBarLabel3.Text = "就绪";
                    timer.Enabled = false;
                    break;

                case LockTypes.Loading:
                    btnLoad.Enabled = false;
                    btnRun.Visible = false;
                    btnStop.Visible = true;
                    btnProfile.Enabled = false;
                    statusBarLabel3.Text = "正在加载配置...";
                    timer.Enabled = true;
                    break;

                case LockTypes.Running:
                    btnLoad.Enabled = false;
                    btnRun.Visible = false;
                    btnStop.Visible = true;
                    btnProfile.Enabled = false;
                    statusBarLabel3.Text = "正在迁移数据...";
                    timer.Enabled = true;
                    break;
            }
        }

        private void ButtonExit_Click(object sender, EventArgs e)
        {
            Close();
        }

        private void ButtonProfile_Click(object sender, EventArgs e)
        {
            new AssistantForm().ShowDialog();
        }

        private void ResetProgress()
        {
            ulong progress = 0, total = 0;

            foreach (ListViewItem item in listView.Items)
                if (item.Tag is Task task)
                {
                    progress += task.Progress;
                    total += task.Total;
                    item.SubItems[1].Text = task.Total.ToString("#,##0");
                    item.SubItems[2].Text = task.Progress.ToString("#,##0");
                    if (task.Total == 0)
                        item.SubItems[3].Text = "N/A";
                    else if (task.Progress >= task.Total)
                        item.SubItems[3].Text = "100.0%";
                    else
                        item.SubItems[3].Text = $"{task.Progress * 100.0 / task.Total:0.0}%";
                    if (task.StartTick == 0)
                        item.SubItems[4].Text = "0.0";
                    else if (task.Status == DataState.Running)
                        item.SubItems[4].Text = ((WinAPI.GetTickCount() - task.StartTick) / 1000.0).ToString("#,##0.0");
                    else
                        item.SubItems[4].Text = (task.StartTick / 1000.0).ToString("#,##0.0");
                    item.StateImageIndex = (int)task.Status;
                }

            if (executor.State == ExecutorState.Planning)
            {
                statusBarLabel2.Text = $"下次执行：{profile.RunTime:M/d HH:mm:ss}";
                statusBarLabel3.Text = "正在等待迁移...";
            }
            else
            {
                if (total == 0)
                    statusBarLabel2.Text = "整体进度：N/A";
                else if (progress >= total)
                    statusBarLabel2.Text = "整体进度：100.0%";
                else
                    statusBarLabel2.Text = $"整体进度：{progress * 100.0 / total:0.0}%";

                if (executor.State == ExecutorState.Testing)
                    statusBarLabel3.Text = "正在检测配置...";
                else if (executor.State == ExecutorState.Running)
                    statusBarLabel3.Text = "正在迁移数据...";
                else
                    statusBarLabel3.Text = "就绪";
            }
        }

        private void ClearView()
        {
            listView.Items.Clear();
            listView.Groups.Clear();
        }

        private void CreateView()
        {
            for (int i = 0; i < profile.Instances.Length; i++)
            {
                ListViewGroup group = new ListViewGroup();

                listView.Groups.Add(group);
                group.Header = profile.Instances[i].Name;

                for (int j = 0; j < profile.Instances[i].Tasks.Length; j++)
                {
                    Task task = profile.Instances[i].Tasks[j];
                    ListViewItem item = group.Items.Add(task.Name, 1);

                    item.Tag = task;
                    item.StateImageIndex = (int)DataState.Normal;
                    item.SubItems.Add("");
                    item.SubItems.Add("");
                    item.SubItems.Add("");
                    item.SubItems.Add("");

                    listView.Items.Add(item);
                }
            }

            statusBarLabel1.Text = $"{profile.Instances.Length} 个实例合共 {listView.Items.Count} 个任务";
            ResetProgress();
        }

        private void AfterAction()
        {
            Invoke(new Action(() =>
            {
                ResetProgress();
                Lock(LockTypes.Ready);
            }));
        }

        private void ButtonLoad_Click(object sender, EventArgs e)
        {
            if (openDialog.ShowDialog() == DialogResult.OK)
            {
                Lock(LockTypes.Loading);
                ClearView();

                try
                {
                    profile.Load(openDialog.FileName);
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"配置文件有误！{ex.Message}", "加载配置", MessageBoxButtons.OK,
                        MessageBoxIcon.Error);
                    Lock(LockTypes.Ready);

                    return;
                }

                CreateView();
                executor.Test(profile);
            }
        }

        private void MainForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (executor.State != ExecutorState.Idle)
            {
                e.Cancel = MessageBox.Show("确定要退出吗？", "确认", MessageBoxButtons.YesNo,
                    MessageBoxIcon.Question) != DialogResult.Yes;

                if (!e.Cancel)
                {
                    executor.Stop();
                    while (executor.State != ExecutorState.Idle)
                    {
                        Thread.Sleep(1);
                        Application.DoEvents();
                    }
                    Thread.Sleep(1);
                }
            }
        }

        private void Timer_Tick(object sender, EventArgs e)
        {
            ResetProgress();
        }

        private void ButtonStop_Click(object sender, EventArgs e)
        {
            executor.Stop();
        }

        private void ButtonRun_Click(object sender, EventArgs e)
        {
            Lock(LockTypes.Running);
            executor.Run(profile);
        }

        private void MainForm_Load(object sender, EventArgs e)
        {
            Text += $" {System.Reflection.Assembly.GetExecutingAssembly().GetName().Version}";
            statusBarLabel1.Text = "未加载配置";
            profile.Callback = AfterAction;
            Lock(LockTypes.Ready);
            ResetProgress();
        }
    }
}

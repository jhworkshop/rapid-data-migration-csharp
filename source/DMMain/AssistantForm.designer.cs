namespace JHWork.DataMigration
{
    partial class AssistantForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(AssistantForm));
            this.tabControl1 = new System.Windows.Forms.TabControl();
            this.tabPage3 = new System.Windows.Forms.TabPage();
            this.runner = new System.Windows.Forms.ComboBox();
            this.label15 = new System.Windows.Forms.Label();
            this.tabPage1 = new System.Windows.Forms.TabPage();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.destEncrypt = new System.Windows.Forms.CheckBox();
            this.destCompress = new System.Windows.Forms.CheckBox();
            this.destCharSet = new System.Windows.Forms.ComboBox();
            this.destPwd = new System.Windows.Forms.TextBox();
            this.destUser = new System.Windows.Forms.TextBox();
            this.destDB = new System.Windows.Forms.TextBox();
            this.destPort = new System.Windows.Forms.TextBox();
            this.destServer = new System.Windows.Forms.TextBox();
            this.destDBMS = new System.Windows.Forms.ComboBox();
            this.label8 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.label10 = new System.Windows.Forms.Label();
            this.label11 = new System.Windows.Forms.Label();
            this.label12 = new System.Windows.Forms.Label();
            this.label13 = new System.Windows.Forms.Label();
            this.label14 = new System.Windows.Forms.Label();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.sourceEncrypt = new System.Windows.Forms.CheckBox();
            this.sourceCompress = new System.Windows.Forms.CheckBox();
            this.sourceCharSet = new System.Windows.Forms.ComboBox();
            this.sourcePwd = new System.Windows.Forms.TextBox();
            this.sourceUser = new System.Windows.Forms.TextBox();
            this.sourceDB = new System.Windows.Forms.TextBox();
            this.sourcePort = new System.Windows.Forms.TextBox();
            this.sourceServer = new System.Windows.Forms.TextBox();
            this.sourceDBMS = new System.Windows.Forms.ComboBox();
            this.label7 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.tabPage2 = new System.Windows.Forms.TabPage();
            this.btnReverse = new System.Windows.Forms.Button();
            this.btnSelectAll = new System.Windows.Forms.Button();
            this.btnRetrieve = new System.Windows.Forms.Button();
            this.listView = new System.Windows.Forms.ListView();
            this.columnHeader1 = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.columnHeader2 = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.columnHeader3 = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.imageList = new System.Windows.Forms.ImageList(this.components);
            this.btnImport = new System.Windows.Forms.Button();
            this.btnSave = new System.Windows.Forms.Button();
            this.openDialog = new System.Windows.Forms.OpenFileDialog();
            this.saveDialog = new System.Windows.Forms.SaveFileDialog();
            this.progressLabel = new System.Windows.Forms.Label();
            this.progressBar = new System.Windows.Forms.ProgressBar();
            this.tabControl1.SuspendLayout();
            this.tabPage3.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox1.SuspendLayout();
            this.tabPage2.SuspendLayout();
            this.SuspendLayout();
            // 
            // tabControl1
            // 
            this.tabControl1.Controls.Add(this.tabPage3);
            this.tabControl1.Controls.Add(this.tabPage1);
            this.tabControl1.Controls.Add(this.tabPage2);
            this.tabControl1.ImageList = this.imageList;
            this.tabControl1.Location = new System.Drawing.Point(8, 8);
            this.tabControl1.Name = "tabControl1";
            this.tabControl1.SelectedIndex = 0;
            this.tabControl1.Size = new System.Drawing.Size(661, 361);
            this.tabControl1.TabIndex = 0;
            // 
            // tabPage3
            // 
            this.tabPage3.Controls.Add(this.runner);
            this.tabPage3.Controls.Add(this.label15);
            this.tabPage3.ImageIndex = 2;
            this.tabPage3.Location = new System.Drawing.Point(4, 31);
            this.tabPage3.Name = "tabPage3";
            this.tabPage3.Size = new System.Drawing.Size(653, 326);
            this.tabPage3.TabIndex = 2;
            this.tabPage3.Text = "执行器";
            this.tabPage3.UseVisualStyleBackColor = true;
            // 
            // runner
            // 
            this.runner.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.runner.FormattingEnabled = true;
            this.runner.Location = new System.Drawing.Point(238, 134);
            this.runner.Name = "runner";
            this.runner.Size = new System.Drawing.Size(200, 25);
            this.runner.TabIndex = 9;
            // 
            // label15
            // 
            this.label15.AutoSize = true;
            this.label15.Location = new System.Drawing.Point(173, 137);
            this.label15.Name = "label15";
            this.label15.Size = new System.Drawing.Size(44, 17);
            this.label15.TabIndex = 8;
            this.label15.Text = "类型：";
            // 
            // tabPage1
            // 
            this.tabPage1.Controls.Add(this.groupBox2);
            this.tabPage1.Controls.Add(this.groupBox1);
            this.tabPage1.ImageIndex = 0;
            this.tabPage1.Location = new System.Drawing.Point(4, 31);
            this.tabPage1.Name = "tabPage1";
            this.tabPage1.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage1.Size = new System.Drawing.Size(653, 326);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Text = "数据源";
            this.tabPage1.UseVisualStyleBackColor = true;
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.destEncrypt);
            this.groupBox2.Controls.Add(this.destCompress);
            this.groupBox2.Controls.Add(this.destCharSet);
            this.groupBox2.Controls.Add(this.destPwd);
            this.groupBox2.Controls.Add(this.destUser);
            this.groupBox2.Controls.Add(this.destDB);
            this.groupBox2.Controls.Add(this.destPort);
            this.groupBox2.Controls.Add(this.destServer);
            this.groupBox2.Controls.Add(this.destDBMS);
            this.groupBox2.Controls.Add(this.label8);
            this.groupBox2.Controls.Add(this.label9);
            this.groupBox2.Controls.Add(this.label10);
            this.groupBox2.Controls.Add(this.label11);
            this.groupBox2.Controls.Add(this.label12);
            this.groupBox2.Controls.Add(this.label13);
            this.groupBox2.Controls.Add(this.label14);
            this.groupBox2.Location = new System.Drawing.Point(332, 16);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(300, 289);
            this.groupBox2.TabIndex = 1;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = " 目标 ";
            // 
            // destEncrypt
            // 
            this.destEncrypt.AutoSize = true;
            this.destEncrypt.Location = new System.Drawing.Point(149, 256);
            this.destEncrypt.Name = "destEncrypt";
            this.destEncrypt.Size = new System.Drawing.Size(75, 21);
            this.destEncrypt.TabIndex = 17;
            this.destEncrypt.Text = "加密传输";
            this.destEncrypt.UseVisualStyleBackColor = true;
            // 
            // destCompress
            // 
            this.destCompress.AutoSize = true;
            this.destCompress.Location = new System.Drawing.Point(19, 256);
            this.destCompress.Name = "destCompress";
            this.destCompress.Size = new System.Drawing.Size(75, 21);
            this.destCompress.TabIndex = 16;
            this.destCompress.Text = "压缩传输";
            this.destCompress.UseVisualStyleBackColor = true;
            // 
            // destCharSet
            // 
            this.destCharSet.FormattingEnabled = true;
            this.destCharSet.Location = new System.Drawing.Point(81, 221);
            this.destCharSet.Name = "destCharSet";
            this.destCharSet.Size = new System.Drawing.Size(200, 25);
            this.destCharSet.TabIndex = 13;
            // 
            // destPwd
            // 
            this.destPwd.Location = new System.Drawing.Point(81, 189);
            this.destPwd.Name = "destPwd";
            this.destPwd.Size = new System.Drawing.Size(200, 23);
            this.destPwd.TabIndex = 12;
            // 
            // destUser
            // 
            this.destUser.Location = new System.Drawing.Point(81, 157);
            this.destUser.Name = "destUser";
            this.destUser.Size = new System.Drawing.Size(200, 23);
            this.destUser.TabIndex = 11;
            // 
            // destDB
            // 
            this.destDB.Location = new System.Drawing.Point(81, 125);
            this.destDB.Name = "destDB";
            this.destDB.Size = new System.Drawing.Size(200, 23);
            this.destDB.TabIndex = 10;
            // 
            // destPort
            // 
            this.destPort.Location = new System.Drawing.Point(81, 93);
            this.destPort.Name = "destPort";
            this.destPort.Size = new System.Drawing.Size(200, 23);
            this.destPort.TabIndex = 9;
            // 
            // destServer
            // 
            this.destServer.Location = new System.Drawing.Point(81, 61);
            this.destServer.Name = "destServer";
            this.destServer.Size = new System.Drawing.Size(200, 23);
            this.destServer.TabIndex = 8;
            // 
            // destDBMS
            // 
            this.destDBMS.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.destDBMS.FormattingEnabled = true;
            this.destDBMS.Location = new System.Drawing.Point(81, 29);
            this.destDBMS.Name = "destDBMS";
            this.destDBMS.Size = new System.Drawing.Size(200, 25);
            this.destDBMS.TabIndex = 7;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(16, 224);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(56, 17);
            this.label8.TabIndex = 6;
            this.label8.Text = "字符集：";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(16, 128);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(56, 17);
            this.label9.TabIndex = 5;
            this.label9.Text = "数据库：";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(16, 32);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(44, 17);
            this.label10.TabIndex = 4;
            this.label10.Text = "类型：";
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Location = new System.Drawing.Point(16, 192);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(44, 17);
            this.label11.TabIndex = 3;
            this.label11.Text = "密码：";
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Location = new System.Drawing.Point(16, 160);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(56, 17);
            this.label12.TabIndex = 2;
            this.label12.Text = "用户名：";
            // 
            // label13
            // 
            this.label13.AutoSize = true;
            this.label13.Location = new System.Drawing.Point(16, 96);
            this.label13.Name = "label13";
            this.label13.Size = new System.Drawing.Size(56, 17);
            this.label13.TabIndex = 1;
            this.label13.Text = "端口号：";
            // 
            // label14
            // 
            this.label14.AutoSize = true;
            this.label14.Location = new System.Drawing.Point(16, 64);
            this.label14.Name = "label14";
            this.label14.Size = new System.Drawing.Size(56, 17);
            this.label14.TabIndex = 0;
            this.label14.Text = "服务器：";
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.sourceEncrypt);
            this.groupBox1.Controls.Add(this.sourceCompress);
            this.groupBox1.Controls.Add(this.sourceCharSet);
            this.groupBox1.Controls.Add(this.sourcePwd);
            this.groupBox1.Controls.Add(this.sourceUser);
            this.groupBox1.Controls.Add(this.sourceDB);
            this.groupBox1.Controls.Add(this.sourcePort);
            this.groupBox1.Controls.Add(this.sourceServer);
            this.groupBox1.Controls.Add(this.sourceDBMS);
            this.groupBox1.Controls.Add(this.label7);
            this.groupBox1.Controls.Add(this.label6);
            this.groupBox1.Controls.Add(this.label5);
            this.groupBox1.Controls.Add(this.label4);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Location = new System.Drawing.Point(16, 16);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(300, 289);
            this.groupBox1.TabIndex = 0;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = " 源 ";
            // 
            // sourceEncrypt
            // 
            this.sourceEncrypt.AutoSize = true;
            this.sourceEncrypt.Location = new System.Drawing.Point(149, 256);
            this.sourceEncrypt.Name = "sourceEncrypt";
            this.sourceEncrypt.Size = new System.Drawing.Size(75, 21);
            this.sourceEncrypt.TabIndex = 15;
            this.sourceEncrypt.Text = "加密传输";
            this.sourceEncrypt.UseVisualStyleBackColor = true;
            // 
            // sourceCompress
            // 
            this.sourceCompress.AutoSize = true;
            this.sourceCompress.Location = new System.Drawing.Point(19, 256);
            this.sourceCompress.Name = "sourceCompress";
            this.sourceCompress.Size = new System.Drawing.Size(75, 21);
            this.sourceCompress.TabIndex = 14;
            this.sourceCompress.Text = "压缩传输";
            this.sourceCompress.UseVisualStyleBackColor = true;
            // 
            // sourceCharSet
            // 
            this.sourceCharSet.FormattingEnabled = true;
            this.sourceCharSet.Location = new System.Drawing.Point(81, 221);
            this.sourceCharSet.Name = "sourceCharSet";
            this.sourceCharSet.Size = new System.Drawing.Size(200, 25);
            this.sourceCharSet.TabIndex = 13;
            // 
            // sourcePwd
            // 
            this.sourcePwd.Location = new System.Drawing.Point(81, 189);
            this.sourcePwd.Name = "sourcePwd";
            this.sourcePwd.Size = new System.Drawing.Size(200, 23);
            this.sourcePwd.TabIndex = 12;
            // 
            // sourceUser
            // 
            this.sourceUser.Location = new System.Drawing.Point(81, 157);
            this.sourceUser.Name = "sourceUser";
            this.sourceUser.Size = new System.Drawing.Size(200, 23);
            this.sourceUser.TabIndex = 11;
            // 
            // sourceDB
            // 
            this.sourceDB.Location = new System.Drawing.Point(81, 125);
            this.sourceDB.Name = "sourceDB";
            this.sourceDB.Size = new System.Drawing.Size(200, 23);
            this.sourceDB.TabIndex = 10;
            // 
            // sourcePort
            // 
            this.sourcePort.Location = new System.Drawing.Point(81, 93);
            this.sourcePort.Name = "sourcePort";
            this.sourcePort.Size = new System.Drawing.Size(200, 23);
            this.sourcePort.TabIndex = 9;
            // 
            // sourceServer
            // 
            this.sourceServer.Location = new System.Drawing.Point(81, 61);
            this.sourceServer.Name = "sourceServer";
            this.sourceServer.Size = new System.Drawing.Size(200, 23);
            this.sourceServer.TabIndex = 8;
            // 
            // sourceDBMS
            // 
            this.sourceDBMS.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.sourceDBMS.FormattingEnabled = true;
            this.sourceDBMS.Location = new System.Drawing.Point(81, 29);
            this.sourceDBMS.Name = "sourceDBMS";
            this.sourceDBMS.Size = new System.Drawing.Size(200, 25);
            this.sourceDBMS.TabIndex = 7;
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(16, 224);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(56, 17);
            this.label7.TabIndex = 6;
            this.label7.Text = "字符集：";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(16, 128);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(56, 17);
            this.label6.TabIndex = 5;
            this.label6.Text = "数据库：";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(16, 32);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(44, 17);
            this.label5.TabIndex = 4;
            this.label5.Text = "类型：";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(16, 192);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(44, 17);
            this.label4.TabIndex = 3;
            this.label4.Text = "密码：";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(16, 160);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(56, 17);
            this.label3.TabIndex = 2;
            this.label3.Text = "用户名：";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(16, 96);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(56, 17);
            this.label2.TabIndex = 1;
            this.label2.Text = "端口号：";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(16, 64);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(56, 17);
            this.label1.TabIndex = 0;
            this.label1.Text = "服务器：";
            // 
            // tabPage2
            // 
            this.tabPage2.Controls.Add(this.btnReverse);
            this.tabPage2.Controls.Add(this.btnSelectAll);
            this.tabPage2.Controls.Add(this.btnRetrieve);
            this.tabPage2.Controls.Add(this.listView);
            this.tabPage2.ImageIndex = 1;
            this.tabPage2.Location = new System.Drawing.Point(4, 31);
            this.tabPage2.Name = "tabPage2";
            this.tabPage2.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage2.Size = new System.Drawing.Size(653, 326);
            this.tabPage2.TabIndex = 1;
            this.tabPage2.Text = "数据表";
            this.tabPage2.UseVisualStyleBackColor = true;
            // 
            // btnReverse
            // 
            this.btnReverse.Location = new System.Drawing.Point(562, 84);
            this.btnReverse.Name = "btnReverse";
            this.btnReverse.Size = new System.Drawing.Size(75, 25);
            this.btnReverse.TabIndex = 4;
            this.btnReverse.Text = "反选(&R)";
            this.btnReverse.UseVisualStyleBackColor = true;
            this.btnReverse.Click += new System.EventHandler(this.ButtonReverse_Click);
            // 
            // btnSelectAll
            // 
            this.btnSelectAll.Location = new System.Drawing.Point(562, 50);
            this.btnSelectAll.Name = "btnSelectAll";
            this.btnSelectAll.Size = new System.Drawing.Size(75, 25);
            this.btnSelectAll.TabIndex = 3;
            this.btnSelectAll.Text = "全选(&A)";
            this.btnSelectAll.UseVisualStyleBackColor = true;
            this.btnSelectAll.Click += new System.EventHandler(this.ButtonSelectAll_Click);
            // 
            // btnRetrieve
            // 
            this.btnRetrieve.Location = new System.Drawing.Point(562, 16);
            this.btnRetrieve.Name = "btnRetrieve";
            this.btnRetrieve.Size = new System.Drawing.Size(75, 25);
            this.btnRetrieve.TabIndex = 2;
            this.btnRetrieve.Text = "提取(&G)";
            this.btnRetrieve.UseVisualStyleBackColor = true;
            this.btnRetrieve.Click += new System.EventHandler(this.ButtonRetrieve_Click);
            // 
            // listView
            // 
            this.listView.CheckBoxes = true;
            this.listView.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.columnHeader1,
            this.columnHeader2,
            this.columnHeader3});
            this.listView.FullRowSelect = true;
            this.listView.GridLines = true;
            this.listView.HideSelection = false;
            this.listView.Location = new System.Drawing.Point(16, 16);
            this.listView.MultiSelect = false;
            this.listView.Name = "listView";
            this.listView.Size = new System.Drawing.Size(530, 293);
            this.listView.SmallImageList = this.imageList;
            this.listView.TabIndex = 0;
            this.listView.UseCompatibleStateImageBehavior = false;
            this.listView.View = System.Windows.Forms.View.Details;
            // 
            // columnHeader1
            // 
            this.columnHeader1.Text = "源";
            this.columnHeader1.Width = 200;
            // 
            // columnHeader2
            // 
            this.columnHeader2.Text = "目标";
            this.columnHeader2.Width = 200;
            // 
            // columnHeader3
            // 
            this.columnHeader3.Text = "排序";
            this.columnHeader3.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
            this.columnHeader3.Width = 80;
            // 
            // imageList
            // 
            this.imageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("imageList.ImageStream")));
            this.imageList.TransparentColor = System.Drawing.Color.Magenta;
            this.imageList.Images.SetKeyName(0, "Server24.bmp");
            this.imageList.Images.SetKeyName(1, "DB24.bmp");
            this.imageList.Images.SetKeyName(2, "Execute24.bmp");
            // 
            // btnImport
            // 
            this.btnImport.Location = new System.Drawing.Point(499, 380);
            this.btnImport.Name = "btnImport";
            this.btnImport.Size = new System.Drawing.Size(75, 25);
            this.btnImport.TabIndex = 1;
            this.btnImport.Text = "导入(&I)";
            this.btnImport.UseVisualStyleBackColor = true;
            this.btnImport.Click += new System.EventHandler(this.ButtonImport_Click);
            // 
            // btnSave
            // 
            this.btnSave.Location = new System.Drawing.Point(590, 380);
            this.btnSave.Name = "btnSave";
            this.btnSave.Size = new System.Drawing.Size(75, 25);
            this.btnSave.TabIndex = 2;
            this.btnSave.Text = "保存(&S)";
            this.btnSave.UseVisualStyleBackColor = true;
            this.btnSave.Click += new System.EventHandler(this.ButtonSave_Click);
            // 
            // openDialog
            // 
            this.openDialog.Filter = "配置文件(*.json)|*.json|所有文件(*.*)|*.*";
            this.openDialog.Title = "导入配置";
            // 
            // saveDialog
            // 
            this.saveDialog.DefaultExt = "json";
            this.saveDialog.Filter = "配置文件(*.json)|*.json|所有文件(*.*)|*.*";
            this.saveDialog.Title = "保存配置";
            // 
            // progressLabel
            // 
            this.progressLabel.AutoSize = true;
            this.progressLabel.Location = new System.Drawing.Point(8, 384);
            this.progressLabel.Name = "progressLabel";
            this.progressLabel.Size = new System.Drawing.Size(125, 17);
            this.progressLabel.TabIndex = 3;
            this.progressLabel.Text = "正在加载数据表信息...";
            this.progressLabel.Visible = false;
            // 
            // progressBar
            // 
            this.progressBar.Location = new System.Drawing.Point(0, 0);
            this.progressBar.Name = "progressBar";
            this.progressBar.Size = new System.Drawing.Size(676, 4);
            this.progressBar.Style = System.Windows.Forms.ProgressBarStyle.Continuous;
            this.progressBar.TabIndex = 4;
            this.progressBar.Visible = false;
            // 
            // AssistantForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 17F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(676, 415);
            this.Controls.Add(this.progressBar);
            this.Controls.Add(this.progressLabel);
            this.Controls.Add(this.btnSave);
            this.Controls.Add(this.btnImport);
            this.Controls.Add(this.tabControl1);
            this.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "AssistantForm";
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "配置助手";
            this.Load += new System.EventHandler(this.ProfileForm_Load);
            this.tabControl1.ResumeLayout(false);
            this.tabPage3.ResumeLayout(false);
            this.tabPage3.PerformLayout();
            this.tabPage1.ResumeLayout(false);
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.tabPage2.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TabControl tabControl1;
        private System.Windows.Forms.TabPage tabPage1;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.TabPage tabPage2;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.ComboBox destCharSet;
        private System.Windows.Forms.TextBox destPwd;
        private System.Windows.Forms.TextBox destUser;
        private System.Windows.Forms.TextBox destDB;
        private System.Windows.Forms.TextBox destPort;
        private System.Windows.Forms.TextBox destServer;
        private System.Windows.Forms.ComboBox destDBMS;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.Label label13;
        private System.Windows.Forms.Label label14;
        private System.Windows.Forms.ComboBox sourceCharSet;
        private System.Windows.Forms.TextBox sourcePwd;
        private System.Windows.Forms.TextBox sourceUser;
        private System.Windows.Forms.TextBox sourceDB;
        private System.Windows.Forms.TextBox sourcePort;
        private System.Windows.Forms.TextBox sourceServer;
        private System.Windows.Forms.ComboBox sourceDBMS;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ImageList imageList;
        private System.Windows.Forms.Button btnImport;
        private System.Windows.Forms.Button btnSave;
        private System.Windows.Forms.Button btnReverse;
        private System.Windows.Forms.Button btnSelectAll;
        private System.Windows.Forms.Button btnRetrieve;
        private System.Windows.Forms.ListView listView;
        private System.Windows.Forms.ColumnHeader columnHeader1;
        private System.Windows.Forms.ColumnHeader columnHeader2;
        private System.Windows.Forms.ColumnHeader columnHeader3;
        private System.Windows.Forms.OpenFileDialog openDialog;
        private System.Windows.Forms.SaveFileDialog saveDialog;
        private System.Windows.Forms.Label progressLabel;
        private System.Windows.Forms.ProgressBar progressBar;
        private System.Windows.Forms.TabPage tabPage3;
        private System.Windows.Forms.ComboBox runner;
        private System.Windows.Forms.Label label15;
        private System.Windows.Forms.CheckBox destEncrypt;
        private System.Windows.Forms.CheckBox destCompress;
        private System.Windows.Forms.CheckBox sourceEncrypt;
        private System.Windows.Forms.CheckBox sourceCompress;
    }
}
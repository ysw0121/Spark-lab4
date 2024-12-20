# Spark-lab4

## 文件说明
按`task1,2,3.py`命名代码文件，输出数据在`res`文件夹下，按各个任务命名。

## Spark环境配置
实验中用`local`模式即可，无需配置集群。因此不涉及其他配置，下载好直接解压即可，压缩包中自带`Scala`环境。解压命令如下：
```
$ sudo tar -zxvf spark-3.5.3-bin-hadoop3.tgz -C/usr/local
```
重命名：
```
$ sudo mv spark-3.5.3-bin-hadoop3 spark
```

对`/etc/profile`和`~/.bashrc`文件进行配置，添加如下内容：
```
export SPARK_HOME=/usr/local/spark
export PATH=$SPARK_HOME/bin:$PATH
```

配置完成后，执行如下命令使配置生效：
```
$ source /etc/profile
$ source ~/.bashrc
```

执行`spark-shell`命令，若出现如下内容，则说明配置成功：
![spark-shell](img/spark-shell.png)
pyspark安装：
```
$ pip install pyspark
```

启动spark-shell: `pyspark`，如下：
![pyspark](img/pyspark.png)

运行示例文件，执行如下：
![run-example](img/pyspark-example.png)

综上，spark环境local模式已完成，可以成功运行。

## 任务一
### 设计思路

### 运行截图

## 任务二
### 设计思路

### 运行截图

## 任务三
### 设计思路

### 运行截图

## 任务四
### 设计思路

### 运行截图


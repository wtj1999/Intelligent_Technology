# Intelligent Technology - 入壳工艺分析系统

基于机器学习和时序数据分析的电池入壳工艺质量监控与分析系统。

## 项目概述

本系统用于电池制造过程中的入壳工艺实时监测、分析和质量诊断。通过采集压力传感器和位置编码器数据，运用时序聚类、异常检测等技术，实现对入壳过程的智能分析和参数推荐。

## 主要功能

### 1. 入壳工艺分析服务 (rk_process_analysis_service)
实时分析入壳过程中的压力和位置数据，判断工艺质量状态。

**主要功能：**
- 4个工位的压力数据实时分析
- 位置序列监测
- 基于DTW（动态时间规整）的时序模式匹配
- 工艺阶段分割与识别
- OK/NG状态判定

**API接口：** `POST /rk/rk-analysis`

### 2. 聚类分析服务 (rk_cluster_analysis_service)
基于历史数据进行KShape时序聚类分析，发现数据模式。

**主要功能：**
- KShape时序聚类算法
- 压力序列和位置序列的聚类分析
- 簇中心和样本标签计算
- 异常样本识别

**API接口：** `POST /rk_cluster/rk-cluster-analysis`

### 3. 模型训练服务 (rk_process_train_service)
使用历史数据训练和更新时序聚类模型。

**主要功能：**
- 从数据库查询历史数据
- 训练KShape聚类模型
- 模型持久化存储

**API接口：** `POST /rk_train/rk-train`

### 4. 参数推荐服务 (rk_param_recommend_service)
基于历史数据的统计分析，推荐工艺参数阈值。

**主要功能：**
- 统计量计算（均值、标准差、分位数）
- 压力参数阈值推荐
- 支持自定义分位数阈值

**API接口：** `POST /rk_recommend/rk-param-recommend`

## 技术栈

- **Web框架**: FastAPI 0.95.2
- **数据计算**: NumPy 2.2.6, Pandas 2.3.2
- **时序分析**: tslearn 0.6.4 (KShape聚类、DTW)
- **数据库**: MySQL (SQLAlchemy 2.0.42, PyMySQL 1.1.1)
- **消息队列**: Kafka (kafka-python 2.2.15)
- **API服务器**: Uvicorn + Gunicorn
- **容器化**: Docker + Docker Compose

## 项目结构

```
Intelligent_Technology/
├── core/                          # 核心模块
│   ├── config.py                  # 配置管理（支持多租户、多环境）
│   └── logging.py                 # 日志配置
├── configs/                       # 配置文件
│   ├── db_config.py              # 数据库配置
│   └── kafka_config.py           # Kafka配置
├── connects/                      # 客户端连接
│   ├── db_client.py              # MySQL数据库客户端
│   └── kafka_client.py           # Kafka生产者客户端
├── services/                      # 业务服务
│   ├── base.py                   # 服务基类
│   ├── factory.py                # 服务工厂（注册、生命周期管理）
│   ├── rk_process_analysis_service/    # 入壳工艺分析服务
│   ├── rk_cluster_analysis_service/    # 聚类分析服务
│   ├── rk_process_train_service/       # 模型训练服务
│   └── rk_param_recommend_service/     # 参数推荐服务
├── main.py                        # FastAPI应用入口
├── requirements.txt               # Python依赖
├── Dockerfile                     # Docker镜像构建
└── docker-compose.yml             # Docker编排配置
```

## 快速开始

### 环境要求

- Python 3.11+
- MySQL 8.0+
- Kafka 2.x

### 本地开发

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 配置环境变量（创建`.env`文件）：
```bash
APP_ENV=test
LOG_LEVEL=INFO
```

3. 启动服务：
```bash
python main.py
```

服务将在 `http://localhost:8000` 启动。

### Docker部署

1. 构建镜像：
```bash
docker build -t intelligent_technology:latest .
```

2. 使用Docker Compose启动：
```bash
docker-compose up -d
```

3. 查看日志：
```bash
docker logs -f intelligent_technology
```

服务将在 `http://localhost:8010` 启动。

## API文档

启动服务后，访问以下地址查看交互式API文档：

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## 配置说明

### 多租户配置

系统支持多租户配置，在 `core/config.py` 中配置：

```python
DB_CONFIG = {
    "tongcheng_rk": {
        "prod": {...},
        "test": {...}
    }
}
```

### 环境切换

通过环境变量 `APP_ENV` 切换环境（`prod`/`test`/`dev`）

## 数据模型

### 入壳工艺分析输入
- `DEVICECODE`: 设备编码
- `PRKDH001-004`: 4个工位的盖板码
- `PRKDH005-012`: 4个工位的压力数据（每个工位2个压力传感器）
- `PRKDH014/016/018/020`: 4个工位的位置数据

### 聚类分析输出
- `labels`: 每个样本的簇标签
- `centers`: 每个簇的中心序列
- `label_counts`: 每个簇的样本数量

## 监控与日志

- 日志文件：`tongcheng_rkod_server.log`
- 日志级别通过 `LOG_LEVEL` 环境变量配置
- 支持邮件告警（在 `core/config.py` 中配置）

## 开发指南

### 添加新服务

1. 在 `services/` 下创建新服务目录
2. 继承 `BaseService` 基类
3. 实现 `startup()`, `shutdown()`, `info()` 方法
4. 在 `main.py` 中注册服务

### 服务工厂使用

```python
from services.factory import get_service_factory

factory = get_service_factory()
factory.register("service_name", ServiceClass)
service = factory.create("service_name")
```

## 许可证

[请根据实际情况添加许可证信息]

## 联系方式

如有问题或建议，请联系开发团队。
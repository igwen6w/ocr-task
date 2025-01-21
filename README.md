# EDOC Task Processor

文档处理任务系统，支持异步处理多种文档任务，包括 OCR 识别和 PDF 处理。

## 功能特性

- 异步任务处理架构
- OCR 图片文字识别
- PDF 文档处理（开发中）
- 支持 Docker 部署

## 环境要求

- Python 3.13.0+
- MySQL 数据库
- Docker（可选）

## 快速开始

### 使用 Docker
1.构建镜像
```bash
docker build -t edoc-task .
```
2.运行容器
```bash
cp .env.example .env
docker compose up -d
#or
docker run -d \
  --name edoc-task \
  -v $(pwd)/config.yml:/app/config.yml \
  edoc-task
```
## 本地开发
1.创建虚拟环境
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
.\venv\Scripts\activate  # Windows
```
2.安装依赖
```bash
pip install -r task/requirements.txt
```
3.配置
- 复制 config.yml.example 为 config.yml
- 修改配置文件中的数据库连接信息和其他设置
4.运行
```bash
python task/main.py
```
## 项目结构
edoc-task/
├── task/
│   ├── main.py          # 主入口
│   ├── pages/           # OCR 处理模块
│   ├── pdfs/           # PDF 处理模块
│   └── config.yml      # 配置文件
├── Dockerfile
└── README.md
## 配置说明
配置文件 config.yml 包含以下主要设置：
- 应用资源文件路径
- 数据库连接信息
- OCR 服务配置
- 任务处理参数
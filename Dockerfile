# 使用 Python 官方的 Docker 镜像作为基础镜像
FROM python:3.13.0
 
# 设置带默认值的环境变量
ARG NAME="default_app"
ENV NAME=${NAME} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
 
# 设置工作目录
WORKDIR /app

# 先安装系统依赖
RUN apt-get update && \
    apt-get upgrade && \
    rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY ./task/requirements.txt .

# 安装应用依赖
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --no-cache-dir --root-user-action=ignore \
    -r requirements.txt 

# 复制应用代码到镜像中的 /app 目录
COPY ./task .

# 运行应用
CMD ["python", "main.py"]
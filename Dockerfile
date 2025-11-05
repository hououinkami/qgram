# ================================
# 第一阶段：构建阶段
# ================================
FROM python:3.11-slim as builder

# 设置构建阶段的环境变量
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 设置工作目录
WORKDIR /app

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 基础编译工具
    gcc \
    g++ \
    make \
    # 加密库依赖
    libffi-dev \
    libssl-dev \
    # 清理
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 升级 pip 和安装构建工具
RUN pip install --upgrade pip setuptools wheel

# 复制 requirements.txt 并安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# ================================
# 第二阶段：运行阶段
# ================================
FROM python:3.11-slim as runtime

# 设置运行时环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

# 设置工作目录
WORKDIR /app

# 只安装运行时必需的依赖（ffmpeg）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 从构建阶段复制已安装的 Python 包
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# 复制项目文件
COPY . .

# 暴露端口
EXPOSE 8088

# 启动主服务
ENTRYPOINT ["python"]
CMD ["main.py"]

# docker build -t qgram:kami .
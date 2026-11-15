FROM python:3.12-slim
WORKDIR /srv/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x ./entrypoint.sh
# 构建产物阶段只建表与回填示例节点；迁移续跑放到容器启动时，保证拿到持久卷上的断点。
RUN python -m app.persistence.cli init
EXPOSE 8000
ENTRYPOINT ["./entrypoint.sh"]

# 基于MinerU和RabbitMQ的PDF解析Worker

- MinerU的GPU镜像构建
- 基于RabbitMQ的PDF异步处理Worker

## 构建方式

```
docker build -t mineru-worker .
```

或者使用代理：

```
docker build --build-arg http_proxy=http://127.0.0.1:7890 --build-arg https_proxy=http://127.0.0.1:7890 -t mineru-worker .
```

## 启动命令

```
docker run --rm -it --gpus=all \
  -e RABBITMQ_HOST=your_rabbitmq_host \
  -e RABBITMQ_QUEUE=mineru_parse_queue \
  -e RABBITMQ_RESULT_QUEUE=mineru_results_queue \
  mineru-worker
```

请确保将 `your_rabbitmq_host` 替换为您的RabbitMQ服务器地址。
# VideoLLM Backend

基于Flask和SocketIO构建的VideoLLM后端服务，支持WebSocket实时通信和模型实例池管理。

## 功能特性

- **WebSocket实时通信**: 支持多客户端同时连接
- **模型实例池管理**: 维护8个模型实例的资源池
- **自动资源管理**: 连接断开时自动释放模型资源
- **负载均衡**: 智能分配可用模型实例
- **实时状态监控**: 提供模型池状态查询接口

## 项目结构

```
backend/
├── app.py              # 主应用文件
├── model_pool.py       # 模型实例池管理
├── config.py          # 配置文件
├── run.py             # 启动脚本
├── requirements.txt   # Python依赖
├── env.example        # 环境变量示例
└── README.md          # 项目说明
```

## 安装和运行

### 1. 安装依赖

```bash
# 创建虚拟环境（推荐）
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
# 复制环境变量示例文件
cp env.example .env

# 编辑 .env 文件，根据需要修改配置
```

### 3. 启动服务

```bash
# 方式1: 使用启动脚本（推荐）
python run.py

# 方式2: 直接运行Flask应用
python app.py

# 方式3: 使用Gunicorn（生产环境）
gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:5000 app:app
```

### 4. 验证服务

访问 `http://localhost:5000` 查看服务状态，或访问 `http://localhost:5000/status` 查看详细状态信息。

## API接口

### HTTP接口

- `GET /` - 服务状态检查
- `GET /status` - 详细状态信息（包含模型池状态）

### WebSocket事件

#### 客户端发送事件

- `connect` - 建立连接（自动获取模型实例）
- `call_model` - 调用模型方法
  ```json
  {
    "method": "generate",
    "args": ["你好"],
    "kwargs": {"max_tokens": 100}
  }
  ```
- `ping` - 心跳检测

#### 服务端发送事件

- `connected` - 连接成功确认
- `model_busy` - 模型池繁忙
- `model_response` - 模型调用结果
- `error` - 错误信息
- `pong` - 心跳响应

## 模型实例池

### 设计原理

- **池大小**: 默认8个模型实例
- **资源分配**: 连接时自动分配，断连时自动释放
- **线程安全**: 使用锁机制保证并发安全
- **超时机制**: 获取模型时支持超时设置

### 模型方法示例

目前支持的模型方法包括：

- `generate(prompt)` - 文本生成
- `analyze_video(video_path)` - 视频分析
- `process_frame(frame_data)` - 帧处理

可以根据实际模型需求扩展更多方法。

## 配置说明

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `SECRET_KEY` | `videollm-secret-key-2024` | Flask密钥 |
| `DEBUG` | `false` | 调试模式 |
| `HOST` | `0.0.0.0` | 服务监听地址 |
| `PORT` | `5000` | 服务端口 |
| `MODEL_POOL_SIZE` | `8` | 模型池大小 |
| `MODEL_ACQUIRE_TIMEOUT` | `0.1` | 模型获取超时时间（秒） |
| `LOG_LEVEL` | `INFO` | 日志级别 |

### 生产环境部署

使用Gunicorn + Nginx部署：

```bash
# 启动Gunicorn
gunicorn --worker-class eventlet -w 1 --bind 127.0.0.1:5000 app:app

# Nginx配置示例
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /socket.io/ {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

## 开发说明

### 添加新的模型方法

在 `model_pool.py` 的 `ModelInstance.call_method()` 方法中添加新的方法处理逻辑：

```python
elif method_name == "your_new_method":
    # 实现你的方法逻辑
    result = your_method_implementation(*args, **kwargs)
    return result
```

### 自定义模型加载

在 `ModelInstance.__init__()` 方法中替换模型加载逻辑：

```python
def __init__(self, model_id: str):
    # 加载你的实际模型
    self.model = load_your_actual_model()
    # 其他初始化代码...
```

## 注意事项

1. **资源管理**: 确保模型的`kill()`方法正确实现资源清理
2. **并发控制**: 单个模型实例同时只能处理一个请求
3. **错误处理**: 模型调用失败不会影响其他客户端
4. **内存管理**: 注意模型实例的内存占用，避免内存泄漏

## 故障排除

### 常见问题

1. **端口占用**: 修改`.env`文件中的`PORT`配置
2. **模型池繁忙**: 增加`MODEL_POOL_SIZE`或优化模型处理速度
3. **连接断开**: 检查网络稳定性和WebSocket配置

### 日志查看

设置环境变量 `LOG_LEVEL=DEBUG` 获取详细日志信息。

## 许可证

MIT License 
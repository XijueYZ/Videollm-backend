import re
from flask import Flask, request
from flask_socketio import SocketIO
from threading import Lock
import logging      
import time
import threading
from model_pool import ModelPool
from config import Config
import base64
from io import BytesIO
from PIL import Image

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)

# 初始化SocketIO - 使用 threading 模式
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading',  # 使用 threading 模式
    logger=False,
    engineio_logger=False,
    ping_timeout=30,
    ping_interval=10,
    max_http_buffer_size=10*1024*1024,  # 10MB 缓冲区
    transports=['websocket', 'polling']
)

# 初始化模型池
model_pool = ModelPool(pool_size=1)

# 简化的状态管理 - 只维护客户端到模型管理器的映射
client_managers = {}
client_lock = Lock()

# 添加客户端activeKey跟踪
client_active_keys = {}  # 跟踪每个客户端的activeKey
active_key_lock = Lock()


@app.route('/')
def index():
    active_count = len(client_managers)
    
    return {
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size,
        'active_connections': active_count,
        'status': model_pool.get_pool_status()
    }

@app.route('/status')
def status():
    active_count = len(client_managers)
    
    return {
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size,
        'active_connections': active_count,
        'status': model_pool.get_pool_status()
    }

@socketio.on('connect')
def handle_connect():
    """处理客户端连接 - 只处理连接，不分配模型"""
    client_id = request.sid
    logger.info(f"🔗 客户端 {client_id} 连接成功")
    
    # 发送连接确认
    socketio.emit('connected', {
        'message': '连接成功，模型分配中',
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size
    }, room=client_id)

@socketio.on('disconnect')
def handle_disconnect():
    """处理客户端断连"""
    client_id = request.sid
    logger.info(f"❌ 客户端 {client_id} 断开连接")
    
    # 检查并释放模型
    manager_to_release = None
    with client_lock:
        if client_id in client_managers:
            manager_to_release = client_managers.pop(client_id)
    
    if manager_to_release:
        try:
            model_pool.release_model(manager_to_release)                # 通知前端模型已释放
            socketio.emit('has_model_released', {
                'available_models': model_pool.available_count()
            })
            logger.info(f"✅ 管理器 {manager_to_release.manager_id} 已释放")
        except Exception as e:
            logger.error(f"❌ 释放管理器失败: {e}")

@socketio.on('request_model')
def handle_request_model(*args):
    """处理模型分配请求 - 新的独立事件"""
    client_id = request.sid
    logger.info(f"📋 客户端 {client_id} 请求模型分配：{args}")

    data = args[0] if args else {}
    
    # 获取activeKey，如果没有传递则使用默认值
    new_active_key = data.get('activeKey', 'None') if isinstance(data, dict) else 'None'
    
    if new_active_key == 'None':
        return
        
    # 检查activeKey是否发生变化
    should_reassign = False
    with active_key_lock:
        old_active_key = client_active_keys.get(client_id)
        if old_active_key != new_active_key:
            logger.info(f"🔄 客户端 {client_id} activeKey变化: {old_active_key} -> {new_active_key}")
            client_active_keys[client_id] = new_active_key
            should_reassign = True
        else:
            logger.info(f"✅ 客户端 {client_id} activeKey未变化: {new_active_key}")
    manager_to_release = None
    with client_lock:
        if client_id in client_managers:
            manager_to_release = client_managers.pop(client_id) 
    
    if manager_to_release:  
    # 如果activeKey发生变化，释放现有模型
        if should_reassign:
            try:
                model_pool.release_model(manager_to_release)
                logger.info(f"🔄 因activeKey变化释放管理器 {manager_to_release.manager_id}")
                
                # 通知前端模型已释放
                socketio.emit('model_released_for_switch', {
                    'old_active_key': old_active_key,
                    'new_active_key': new_active_key,
                    'message': f'切换到 {new_active_key}，正在重新分配模型...'
                }, room=client_id)
                
            except Exception as e:
                logger.error(f"❌ 释放管理器失败: {e}")
        
    
        # 检查是否已经有模型（activeKey相同时）
        else:
            return

    def assign_model():
        try:
            # 通知开始分配
            socketio.emit('model_assigning', {
                'message': '正在分配模型...',
                'available_models': model_pool.available_count()
            }, room=client_id)
            
            # 尝试获取模型 - 可以设置更长的超时时间
            manager = model_pool.acquire_model(timeout=30)  # 30秒超时
            
            if manager is None:
                socketio.emit('model_assign_failed', {
                    'message': '模型池繁忙，请稍后重试',
                    'available_models': model_pool.available_count()
                }, room=client_id)
                return
            
            # 定义token回调函数
            def token_callback(token):
                try:
                    socketio.emit('new_token', {'token': token}, room=client_id)
                    logger.debug(f"发送token给客户端 {client_id}: {token}")
                except Exception as e:
                    logger.error(f"发送token失败: {e}")
                    # 如果发送失败，说明客户端可能已断开
                    with client_lock:
                        if client_id in client_managers:
                            del client_managers[client_id]
                    model_pool.release_model(manager)
            
            # 启动模型会话
            manager.start_session(token_callback)
            
            # 保存映射关系
            with client_lock:
                client_managers[client_id] = manager
            
            logger.info(f"✅ 客户端 {client_id} 成功分配到管理器 {manager.manager_id}")
            
            # 通知分配成功
            socketio.emit('model_assigned', {
                'success': True,
                'model_id': manager.manager_id,
                'message': '模型分配成功',
                'available_models': model_pool.available_count()
            }, room=client_id)
            
        except Exception as e:
            logger.error(f"❌ 为客户端 {client_id} 分配模型失败: {e}")
            socketio.emit('model_assign_failed', {
                'message': f'模型分配失败: {str(e)}',
                'available_models': model_pool.available_count()
            }, room=client_id)

    # 启动后台线程进行模型分配
    threading.Thread(target=assign_model, daemon=True).start()

@socketio.on('send_data')
def handle_send_data(data):
    """统一处理发送数据请求（支持文本、图片或两者）"""
    client_id = request.sid
    
    # 快速获取manager（优化锁使用）
    manager = client_managers.get(client_id)
    if manager is None:
        socketio.emit('error', {'message': '模型池繁忙，请稍后重试'}, room=client_id)
        return
    
    try:
        message = data.get('message', '').strip()
        image_data = data.get('image_data')
        
        # 验证至少有一种数据
        if not message and not image_data:
            socketio.emit('error', {'message': '请提供文本消息或图片数据'}, room=client_id)
            return
        
        # 处理图片（如果有）
        image = None
        if image_data:
            try:
                base64_data = re.sub('^data:image/.+;base64,', '', image_data)
                image_bytes = base64.b64decode(base64_data)
                image = Image.open(BytesIO(image_bytes))
                logger.info(f"🖼️ 客户端 {client_id} 图片解码成功")
            except Exception as e:
                logger.error(f"❌ 图片解码失败: {e}")
                socketio.emit('error', {'message': '图片解码失败'}, room=client_id)
                return
        
        # 记录请求类型
        if message and image:
            logger.info(f"📝🖼️ 客户端 {client_id} 发送文本+图片: {message[:30]}...")
        elif message:
            logger.info(f"📝 客户端 {client_id} 发送消息: {message[:50]}...")
        elif image:
            logger.info(f"🖼️ 客户端 {client_id} 发送图片分析请求")
        
        # 按顺序添加到队列
        if message:
            manager.add_prompt(message)
        if image:
            manager.add_image(image)
            
    except Exception as e:
        logger.error(f"❌ 处理数据失败: {e}")
        socketio.emit('error', {'message': f'数据处理失败: {str(e)}'}, room=client_id)

if __name__ == '__main__':
    logger.info("🚀 启动VideoLLM后端服务...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
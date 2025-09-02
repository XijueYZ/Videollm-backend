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
    with client_lock:
        active_count = len(client_managers)
    
    return {
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size,
        'active_connections': active_count,
        'status': model_pool.get_pool_status()
    }

@app.route('/status')
def status():
    with client_lock:
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
    """处理客户端断连 - 简化版本"""
    client_id = request.sid
    logger.info(f"❌ 客户端 {client_id} 断开连接")
    
    # 检查并释放模型
    manager_to_release = None
    with client_lock:
        if client_id in client_managers:
            manager_to_release = client_managers.pop(client_id)
    
    if manager_to_release:
        try:
            model_pool.release_model(manager_to_release)
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
    
        # 如果activeKey发生变化，释放现有模型
    if should_reassign:
        manager_to_release = None
        with client_lock:
            if client_id in client_managers:
                manager_to_release = client_managers.pop(client_id)
        
        if manager_to_release:
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
    if not should_reassign:
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

@socketio.on('release_model')
def handle_release_model():
    """处理模型释放请求 - 可选的显式释放"""
    client_id = request.sid
    logger.info(f"🔄 客户端 {client_id} 请求释放模型")
    
    manager_to_release = None
    with client_lock:
        if client_id in client_managers:
            manager_to_release = client_managers.pop(client_id)
    
    if manager_to_release:
        try:
            model_pool.release_model(manager_to_release)
            logger.info(f"✅ 管理器 {manager_to_release.manager_id} 已释放")
            socketio.emit('model_released', {
                'success': True,
                'message': '模型已释放'
            }, room=client_id)
        except Exception as e:
            logger.error(f"❌ 释放管理器失败: {e}")
            socketio.emit('model_released', {
                'success': False,
                'message': f'释放失败: {str(e)}'
            }, room=client_id)
    else:
        socketio.emit('model_released', {
            'success': False,
            'message': '没有分配的模型'
        }, room=client_id)

@socketio.on('send_message')
def handle_send_message(data):
    """处理发送消息请求"""
    client_id = request.sid
    
    # 检查是否有分配的模型
    with client_lock:
        if client_id not in client_managers:
            socketio.emit('error', {'message': '请先请求模型分配'}, room=client_id)
            return
        manager = client_managers[client_id]
    
    try:
        message = data.get('message', '')
        if not message:
            socketio.emit('error', {'message': '消息内容不能为空'}, room=client_id)
            return
        
        logger.info(f"📝 客户端 {client_id} 发送消息: {message[:50]}...")
        
        # 将消息添加到管理器的prompt队列
        manager.add_prompt(message)
        
    except Exception as e:
        logger.error(f"❌ 处理消息失败: {e}")
        socketio.emit('error', {'message': f'消息处理失败: {str(e)}'}, room=client_id)

@socketio.on('send_image')
def handle_send_image(data):
    """处理发送图片请求"""
    client_id = request.sid
    
    # 检查是否有分配的模型
    with client_lock:
        if client_id not in client_managers:
            socketio.emit('error', {'message': '请先请求模型分配'}, room=client_id)
            return
        manager = client_managers[client_id]
    
    try:
        image_data = data.get('image_data')
        prompt = data.get('prompt', '')
        
        if not image_data:
            socketio.emit('error', {'message': '图片数据不能为空'}, room=client_id)
            return
        
        # Convert base64 image data to PIL format
        try:
            base64_data = re.sub('^data:image/.+;base64,', '', image_data)
            image_bytes = base64.b64decode(base64_data)
            image = Image.open(BytesIO(image_bytes))
        except Exception as e:
            logger.error(f"❌ 图片解码失败: {e}")
            socketio.emit('error', {'message': '图片解码失败'}, room=client_id)
            return
        
        logger.info(f"🖼️ 客户端 {client_id} 发送图片分析请求")
        
        # 先添加prompt（如果有），再添加图片
        if prompt:
            manager.add_prompt(prompt)
        manager.add_image(image)
        
    except Exception as e:
        logger.error(f"❌ 处理图片失败: {e}")
        socketio.emit('error', {'message': f'图片处理失败: {str(e)}'}, room=client_id)

if __name__ == '__main__':
    logger.info("🚀 启动VideoLLM后端服务...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
import re
from flask import Flask, request
from flask_socketio import SocketIO, emit, disconnect
from threading import Lock
import logging
import uuid
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

# 初始化SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# 初始化模型池
model_pool = ModelPool(pool_size=1)

# 存储客户端连接和模型管理器的映射关系
client_managers = {}
# 存储等待模型的客户端队列  
waiting_clients = {}
# 新增：跟踪活跃的客户端连接状态
active_clients = set()
client_lock = Lock()


@app.route('/')
def index():
    with client_lock:
        waiting_count = len(waiting_clients)
        active_count = len(client_managers)
    
    return {
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size,
        'active_connections': active_count,
        'waiting_connections': waiting_count,
        'status': model_pool.get_pool_status()
    }


@app.route('/status')
def status():
    with client_lock:
        waiting_count = len(waiting_clients)
        active_count = len(client_managers)
    
    return {
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size,
        'active_connections': active_count,
        'waiting_connections': waiting_count,
        'status': model_pool.get_pool_status()
    }


def try_assign_model_to_waiting_clients():
    """尝试为等待中的客户端分配模型"""
    with client_lock:
        if not waiting_clients:
            return
        
        # 获取等待时间最长的客户端
        oldest_client = min(waiting_clients.keys(), 
                          key=lambda cid: waiting_clients[cid]['wait_start'])
        
        # 尝试获取模型
        manager = model_pool.acquire_model(timeout=0.1)
        if manager is None:
            return  # 仍然没有可用模型
        
        client_id = oldest_client
        client_info = waiting_clients.pop(client_id)
        
        logger.info(f"为等待的客户端 {client_id} 分配模型 {manager.manager_id}")
        
        try:
            # 定义token回调函数
            def token_callback(token):
                """当模型生成新token时的回调"""
                try:
                    socketio.emit('new_token', {'token': token}, room=client_id)
                    logger.debug(f"发送token给客户端 {client_id}: {token}")
                except Exception as e:
                    logger.error(f"发送token失败: {e}")
                    with client_lock:
                        active_clients.discard(client_id)
            
            # 启动模型会话
            manager.start_session(token_callback)
            
            # 保存客户端和管理器的映射关系
            client_managers[client_id] = manager
            
            wait_time = time.time() - client_info['wait_start']
            logger.info(f"客户端 {client_id} 等待 {wait_time:.2f}秒后成功分配到管理器 {manager.manager_id}")
            
            # 通知客户端分配成功
            try:
                socketio.emit('connected', {
                    'message': '连接成功',
                    'model_id': manager.manager_id,
                    'available_models': model_pool.available_count(),
                    'wait_time': wait_time
                }, room=client_id)
            except (ConnectionError, Exception) as e:
                logger.error(f"向客户端 {client_id} 发送连接成功消息失败: {e}")
                # 如果连接已断开，清理资源
                with client_lock:
                    active_clients.discard(client_id)
                    if client_id in client_managers:
                        del client_managers[client_id]
                model_pool.release_model(manager)
                # 模型释放后，检查等待队列
                try_assign_model_to_waiting_clients()
                return
            
        except Exception as e:
            logger.error(f"为客户端 {client_id} 分配模型失败: {e}")
            
            try:
                socketio.emit('error', {'message': f'模型分配失败: {str(e)}'}, room=client_id)
            except (ConnectionError, Exception) as emit_error:
                logger.error(f"向客户端 {client_id} 发送错误消息失败: {emit_error}")
                with client_lock:
                    active_clients.discard(client_id)


@socketio.on('connect')
def handle_connect():
    """处理客户端连接"""
    client_id = request.sid
    logger.info(f"客户端 {client_id} 尝试连接")
    
    # 将客户端添加到活跃连接集合
    with client_lock:
        active_clients.add(client_id)
    
    try:
        # 尝试获取模型管理器
        logger.info(f"客户端 {client_id} 正在获取模型管理器...")
        manager = model_pool.acquire_model(timeout=0.1)
        logger.info(f"客户端 {client_id} 获取模型结果: {manager.manager_id if manager else 'None'}")
        
        
        if manager is None:
            # 没有可用模型，加入等待队列
            with client_lock:
                waiting_clients[client_id] = {
                    'wait_start': time.time(),
                    'connect_time': time.time()
                }
            
            logger.info(f"客户端 {client_id} 加入等待队列，当前等待人数: {len(waiting_clients)}")
            
            # 通知客户端正在等待
            try:
                socketio.emit('waiting_for_model', {
                    'message': '模型池繁忙，正在等待空闲模型...',
                    'queue_position': len(waiting_clients),
                    'available_models': model_pool.available_count(),
                    'total_models': model_pool.pool_size
                }, room=client_id)
            except (ConnectionError, Exception) as e:
                logger.error(f"向等待客户端 {client_id} 发送等待消息失败: {e}")
                with client_lock:
                    active_clients.discard(client_id)
                    if client_id in waiting_clients:
                        del waiting_clients[client_id]
                return
            
            return  # 不断开连接，保持等待状态
        
        # 有可用模型，直接分配
        # 定义token回调函数
        def token_callback(token):
            """当模型生成新token时的回调"""
            try:
                # 检查客户端是否仍然连接
                with client_lock:
                    if client_id not in active_clients:
                        logger.debug(f"客户端 {client_id} 已断开，跳过token发送: {token}")
                        return
                
                socketio.emit('new_token', {'token': token}, room=client_id)
                logger.debug(f"发送token给客户端 {client_id}: {token}")
            except ConnectionError:
                logger.warning(f"客户端 {client_id} 连接已断开，停止发送token")
                with client_lock:
                    active_clients.discard(client_id)
            except Exception as e:
                logger.error(f"发送token失败: {e}")
                # 如果是连接相关错误，从活跃连接中移除
                if "connection" in str(e).lower() or "broken pipe" in str(e).lower():
                    with client_lock:
                        active_clients.discard(client_id)
        
        # 启动模型会话
        manager.start_session(token_callback)
        
        # 保存客户端和管理器的映射关系
        with client_lock:
            client_managers[client_id] = manager
        
        logger.info(f"客户端 {client_id} 连接成功，直接分配管理器 {manager.manager_id}")
        
        # 修复：使用 socketio.emit 而不是 emit
        try:
            socketio.emit('connected', {
                'message': '连接成功',
                'model_id': manager.manager_id,
                'available_models': model_pool.available_count(),
                'wait_time': 0
            }, room=client_id)
        except (ConnectionError, Exception) as e:
            logger.error(f"向客户端 {client_id} 发送连接成功消息失败: {e}")
            # 如果连接已断开，清理资源
            with client_lock:
                active_clients.discard(client_id)
                if client_id in client_managers:
                    del client_managers[client_id]
            model_pool.release_model(manager)
            # 模型释放后，检查等待队列
            try_assign_model_to_waiting_clients()
            return
        
    except Exception as e:
        logger.error(f"处理客户端连接失败: {e}")
        # 如果连接失败，从活跃连接中移除
        with client_lock:
            active_clients.discard(client_id)
        try:
            socketio.emit('error', {'message': f'连接失败: {str(e)}'}, room=client_id)
        except:
            logger.error(f"发送错误消息失败，客户端 {client_id} 可能已断开")


@socketio.on('disconnect')
def handle_disconnect():
    """处理客户端断连"""
    client_id = request.sid
    logger.info(f"客户端 {client_id} 断开连接")
    
    with client_lock:
        # 立即从活跃连接集合中移除，防止后续token回调
        active_clients.discard(client_id)
        
        # 检查是否在等待队列中
        if client_id in waiting_clients:
            wait_time = time.time() - waiting_clients[client_id]['wait_start']
            logger.info(f"客户端 {client_id} 在等待 {wait_time:.2f}秒后断开连接")
            del waiting_clients[client_id]
            return
        
        # 检查是否有分配的管理器
        if client_id in client_managers:
            manager = client_managers[client_id]
            try:
                # 释放模型管理器（会自动调用stop_session）
                model_pool.release_model(manager)
                logger.info(f"管理器 {manager.manager_id} 已释放")
                
                # 模型释放后，立即检查是否有等待的客户端可以分配
                try_assign_model_to_waiting_clients()
                
            except Exception as e:
                logger.error(f"释放管理器 {manager.manager_id} 失败: {e}")
            
            del client_managers[client_id]
            logger.info(f"客户端 {client_id} 的管理器已释放回池中")


@socketio.on('send_message')
def handle_send_message(data):
    """处理发送消息请求"""
    client_id = request.sid
    
    with client_lock:
        # 检查客户端是否在等待队列中
        if client_id in waiting_clients:
            socketio.emit('error', {'message': '正在等待模型分配，请稍后再试'}, room=client_id)
            return
        
        if client_id not in client_managers:
            socketio.emit('error', {'message': '未找到分配的模型管理器'}, room=client_id)
            return
        
        manager = client_managers[client_id]
    
    try:
        message = data.get('message', '')
        if not message:
            socketio.emit('error', {'message': '消息内容不能为空'}, room=client_id)
            return
        
        logger.info(f"客户端 {client_id} 发送消息: {message[:50]}...")
        
        # 将消息添加到管理器的prompt队列
        manager.add_prompt(message)
        
    except Exception as e:
        logger.error(f"处理消息失败: {e}")
        socketio.emit('error', {'message': f'消息处理失败: {str(e)}'}, room=client_id)


@socketio.on('send_image')
def handle_send_image(data):
    """处理发送图片请求"""
    client_id = request.sid
    
    with client_lock:
        # 检查客户端是否在等待队列中
        if client_id in waiting_clients:
            socketio.emit('error', {'message': '正在等待模型分配，请稍后再试'}, room=client_id)
            return
        
        if client_id not in client_managers:
            socketio.emit('error', {'message': '未找到分配的模型管理器'}, room=client_id)
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
            # Remove data URL prefix if present
            base64_data = re.sub('^data:image/.+;base64,', '', image_data)
            image_bytes = base64.b64decode(base64_data)
            image = Image.open(BytesIO(image_bytes))
            
        except Exception as e:
            logger.error(f"图片解码失败: {e}")
            try:
                socketio.emit('error', {'message': '图片解码失败'}, room=client_id)
            except Exception as emit_error:
                logger.error(f"发送错误消息失败: {emit_error}")
            return
        
        logger.info(f"客户端 {client_id} 发送图片分析请求")
        
        # 先添加prompt（如果有），再添加图片
        if prompt:
            manager.add_prompt(prompt)
        manager.add_image(image)
        
    except Exception as e:
        logger.error(f"处理图片失败: {e}")
        socketio.emit('error', {'message': f'图片处理失败: {str(e)}'}, room=client_id)


if __name__ == '__main__':
    logger.info("启动VideoLLM后端服务...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
from flask import Flask, request
from flask_socketio import SocketIO, emit, disconnect
from threading import Lock
import logging
import uuid
import time
import threading
from model_pool import ModelPool
from config import Config

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)

# 初始化SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# 初始化模型池
model_pool = ModelPool(pool_size=8)

# 存储客户端连接和模型管理器的映射关系
client_managers = {}
# 存储等待模型的客户端队列
waiting_clients = {}
client_lock = Lock()


@app.route('/')
def index():
    return {'status': 'VideoLLM Backend Running', 'available_models': model_pool.available_count()}


@app.route('/status')
def status():
    with client_lock:
        waiting_count = len(waiting_clients)
        active_count = len(client_managers)
    
    return {
        'available_models': model_pool.available_count(),
        'total_models': model_pool.pool_size,
        'active_connections': active_count,
        'waiting_connections': waiting_count
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
            
            # 启动模型会话
            manager.start_session(token_callback)
            
            # 保存客户端和管理器的映射关系
            client_managers[client_id] = manager
            
            wait_time = time.time() - client_info['wait_start']
            logger.info(f"客户端 {client_id} 等待 {wait_time:.2f}秒后成功分配到管理器 {manager.manager_id}")
            
            # 通知客户端分配成功
            socketio.emit('connected', {
                'message': '连接成功',
                'model_id': manager.manager_id,
                'available_models': model_pool.available_count(),
                'wait_time': wait_time
            }, room=client_id)
            
        except Exception as e:
            logger.error(f"为等待客户端分配模型失败: {e}")
            # 模型分配失败，放回池中
            model_pool.release_model(manager)
            # 重新加入等待队列
            waiting_clients[client_id] = client_info
            socketio.emit('error', {'message': f'模型分配失败: {str(e)}'}, room=client_id)


@socketio.on('connect')
def handle_connect():
    """处理客户端连接"""
    client_id = request.sid
    logger.info(f"客户端 {client_id} 尝试连接")
    
    try:
        # 尝试获取模型管理器
        manager = model_pool.acquire_model(timeout=0.1)
        
        
        if manager is None:
            # 没有可用模型，加入等待队列
            with client_lock:
                waiting_clients[client_id] = {
                    'wait_start': time.time(),
                    'connect_time': time.time()
                }
            
            logger.info(f"客户端 {client_id} 加入等待队列，当前等待人数: {len(waiting_clients)}")
            
            # 通知客户端正在等待
            socketio.emit('waiting_for_model', {
                'message': '模型池繁忙，正在等待空闲模型...',
                'queue_position': len(waiting_clients),
                'available_models': model_pool.available_count(),
                'total_models': model_pool.pool_size
            })
            
            # 启动定期检查，为等待的客户端分配模型
            def check_and_assign():
                time.sleep(1)  # 延迟1秒再检查
                try_assign_model_to_waiting_clients()
            
            threading.Thread(target=check_and_assign, daemon=True).start()
            return  # 不断开连接，保持等待状态
        
        # 有可用模型，直接分配
        # 定义token回调函数
        def token_callback(token):
            """当模型生成新token时的回调"""
            try:
                # 使用 socketio.emit() 而不是 emit()，并指定 room
                socketio.emit('new_token', {'token': token}, room=client_id)
                logger.debug(f"发送token给客户端 {client_id}: {token}")
            except Exception as e:
                logger.error(f"发送token失败: {e}")
        
        # 启动模型会话
        manager.start_session(token_callback)
        
        # 保存客户端和管理器的映射关系
        with client_lock:
            client_managers[client_id] = manager
        
        logger.info(f"客户端 {client_id} 连接成功，直接分配管理器 {manager.manager_id}")
        
        # 修复：使用 socketio.emit 而不是 emit
        socketio.emit('connected', {
            'message': '连接成功',
            'model_id': manager.manager_id,
            'available_models': model_pool.available_count(),
            'wait_time': 0
        })
        
    except Exception as e:
        logger.error(f"处理客户端连接失败: {e}")
        emit('error', {'message': f'连接失败: {str(e)}'})


@socketio.on('disconnect')
def handle_disconnect():
    """处理客户端断连"""
    client_id = request.sid
    logger.info(f"客户端 {client_id} 断开连接")
    
    with client_lock:
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
            emit('error', {'message': '正在等待模型分配，请稍后再试'})
            return
        
        if client_id not in client_managers:
            emit('error', {'message': '未找到分配的模型管理器'})
            return
        
        manager = client_managers[client_id]
    
    try:
        message = data.get('message', '')
        if not message:
            emit('error', {'message': '消息内容不能为空'})
            return
        
        logger.info(f"客户端 {client_id} 发送消息: {message[:50]}...")
        
        # 将消息添加到管理器的prompt队列
        manager.add_prompt(message)
        
    except Exception as e:
        logger.error(f"处理消息失败: {e}")
        emit('error', {'message': f'消息处理失败: {str(e)}'})


@socketio.on('send_image')
def handle_send_image(data):
    """处理发送图片请求"""
    client_id = request.sid
    
    with client_lock:
        # 检查客户端是否在等待队列中
        if client_id in waiting_clients:
            emit('error', {'message': '正在等待模型分配，请稍后再试'})
            return
        
        if client_id not in client_managers:
            emit('error', {'message': '未找到分配的模型管理器'})
            return
        
        manager = client_managers[client_id]
    
    try:
        image_data = data.get('image_data')
        prompt = data.get('prompt', '')
        
        if not image_data:
            emit('error', {'message': '图片数据不能为空'})
            return
        
        logger.info(f"客户端 {client_id} 发送图片分析请求")
        
        # 先添加prompt（如果有），再添加图片
        if prompt:
            manager.add_prompt(prompt)
        manager.add_image(image_data)
        
    except Exception as e:
        logger.error(f"处理图片失败: {e}")
        emit('error', {'message': f'图片处理失败: {str(e)}'})


if __name__ == '__main__':
    logger.info("启动VideoLLM后端服务...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
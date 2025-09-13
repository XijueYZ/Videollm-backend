import re
import os
import uuid
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

# 创建临时文件夹
TEMP_DIR = os.path.join(os.getcwd(), 'temp_uploads')
os.makedirs(TEMP_DIR, exist_ok=True)

def cleanup_temp_files(client_id=None, max_age_hours=24):
    """清理临时文件"""
    try:
        current_time = time.time()
        for filename in os.listdir(TEMP_DIR):
            filepath = os.path.join(TEMP_DIR, filename)
            
            # 如果指定了client_id，只清理该客户端的文件
            if client_id and not filename.startswith(f"{client_id}_"):
                continue
                
            # 检查文件年龄
            file_age = current_time - os.path.getctime(filepath)
            if file_age > max_age_hours * 3600:  # 转换为秒
                os.remove(filepath)
                logger.info(f"🗑️ 清理过期临时文件: {filename}")
                
    except Exception as e:
        logger.error(f"❌ 清理临时文件失败: {e}")

# 启动定期清理任务
def start_cleanup_task():
    """启动定期清理任务"""
    def cleanup_worker():
        while True:
            time.sleep(3600)  # 每小时清理一次
            cleanup_temp_files()
    
    threading.Thread(target=cleanup_worker, daemon=True).start()

start_cleanup_task()

# 初始化SocketIO - 使用 threading 模式
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading',  # 使用 threading 模式
    logger=False,
    engineio_logger=False,
    ping_timeout=30,
    ping_interval=10,
    max_http_buffer_size=100*1024*1024,  # 100MB 缓冲区，支持大视频文件
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
    with active_key_lock:
        client_active_keys.pop(client_id)
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
    
    # 清理该客户端的临时文件
    cleanup_temp_files(client_id=client_id)

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
            manager.start_session(token_callback, new_active_key)
            
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

@socketio.on_error()
def error_handler(e):
    """全局SocketIO错误处理器"""
    client_id = request.sid if request else 'unknown'
    logger.error(f"❌ SocketIO错误 (客户端 {client_id}): {e}")
    logger.error(f"❌ 错误类型: {type(e)}")
    import traceback
    logger.error(f"❌ 错误堆栈: {traceback.format_exc()}")

@socketio.on('send_data')
def handle_send_data(data):
    """统一处理发送数据请求（支持文本、图片或两者）"""
    client_id = request.sid
    logger.info(f"📨 收到客户端 {client_id} 的 send_data 请求")
    
    # 添加数据大小检查
    try:
        import sys
        data_size = sys.getsizeof(data)
        logger.info(f"📊 数据大小: {data_size / 1024 / 1024:.2f} MB")
    except Exception as e:
        logger.warning(f"⚠️ 无法计算数据大小: {e}")
    
    # 快速获取manager（优化锁使用）
    manager = client_managers.get(client_id)
    if manager is None:
        socketio.emit('error', {'message': '模型池繁忙，请稍后重试'}, room=client_id)
        return
    
    try:
        send_type = data.get('type', 'chat')
        message = data.get('message', '').strip()
        image_data = data.get('image_data')
        images = data.get('images')
        videos = data.get('videos')
        params = data.get('params')
        
        # 调试信息：打印接收到的数据类型
        if images:
            logger.info(f"🔍 调试 - images类型: {type(images)}, 长度: {len(images) if hasattr(images, '__len__') else 'N/A'}")
            for i, img in enumerate(images[:3]):  # 只打印前3个
                logger.info(f"🔍 调试 - images[{i}]类型: {type(img)}, 大小: {len(img) if isinstance(img, (bytes, str)) else 'N/A'}")
        if videos:
            logger.info(f"🔍 调试 - videos类型: {type(videos)}, 长度: {len(videos) if hasattr(videos, '__len__') else 'N/A'}")
            for i, vid in enumerate(videos[:3]):  # 只打印前3个
                if isinstance(vid, dict):
                    logger.info(f"🔍 调试 - videos[{i}]类型: {type(vid)}, 键: {list(vid.keys())}")
                    if 'data' in vid:
                        data_type = type(vid['data'])
                        data_size = len(vid['data']) if hasattr(vid['data'], '__len__') else 'N/A'
                        logger.info(f"🔍 调试 - videos[{i}].data类型: {data_type}, 大小: {data_size}")
                else:
                    logger.info(f"🔍 调试 - videos[{i}]类型: {type(vid)}, 大小: {len(vid) if isinstance(vid, (bytes, str)) else 'N/A'}")
        
        # 验证至少有一种数据
        if send_type == 'stream':
            if not message and not image_data:
                socketio.emit('error', {'message': '请提供文本消息或图片数据'}, room=client_id)
                return
        elif send_type == 'chat':
            if not message and not images and not videos:
                socketio.emit('error', {'message': '请提供文本消息、图片或视频数据'}, room=client_id)
                return
        
        # 处理图片（如果有）
        if send_type == 'stream':
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
        
        elif send_type == 'chat':
            processed_images = []
            processed_videos = []
            
            # 把image从File转化为Image
            if images:
                for i, img_file in enumerate(images):
                    try:
                        image = None
                        filename = f"image_{i}"
                        
                        # 处理不同类型的图片数据
                        if hasattr(img_file, 'stream') and hasattr(img_file, 'filename'):
                            # 这是一个FileStorage对象
                            image = Image.open(img_file.stream)
                            filename = img_file.filename
                            logger.info(f"🖼️ 客户端 {client_id} 图片处理成功 (FileStorage): {filename}")
                        elif isinstance(img_file, bytes):
                            # 这是bytes数据
                            image = Image.open(BytesIO(img_file))
                            logger.info(f"🖼️ 客户端 {client_id} 图片处理成功 (bytes): {filename}")
                        elif isinstance(img_file, str):
                            # 这可能是base64数据
                            try:
                                base64_data = re.sub('^data:image/.+;base64,', '', img_file)
                                image_bytes = base64.b64decode(base64_data)
                                image = Image.open(BytesIO(image_bytes))
                                logger.info(f"🖼️ 客户端 {client_id} 图片处理成功 (base64): {filename}")
                            except Exception as e:
                                logger.error(f"❌ base64图片解码失败: {e}")
                                continue
                        else:
                            logger.warning(f"⚠️ 不支持的图片格式: {type(img_file)}")
                            continue
                        
                        if image:
                            processed_images.append(image)
                            
                    except Exception as e:
                        logger.error(f"❌ 图片处理失败: {e}")
                        continue
            
            # 把video从File保存到本地某个临时文件夹里，带request.sid
            if videos:
                for i, video_file in enumerate(videos):
                    try:
                        original_filename = f"video_{i}.mp4"
                        file_ext = '.mp4'
                        video_data = None
                        
                        # 处理不同类型的视频数据
                        if hasattr(video_file, 'stream') and hasattr(video_file, 'filename'):
                            # 这是一个FileStorage对象
                            original_filename = video_file.filename
                            file_ext = os.path.splitext(original_filename)[1] if original_filename else '.mp4'
                            video_file.stream.seek(0)  # 确保从头开始读取
                            video_data = video_file.stream.read()
                            logger.info(f"🎥 客户端 {client_id} 视频处理成功 (FileStorage): {original_filename}")
                        elif isinstance(video_file, dict) and 'data' in video_file:
                            # 这是前端发送的对象格式: {name, type, size, data}
                            original_filename = video_file.get('name', f'video_{i}.mp4')
                            file_type = video_file.get('type', 'video/mp4')
                            file_size = video_file.get('size', 0)
                            
                            # 从MIME类型推断文件扩展名
                            if file_type.startswith('video/'):
                                mime_type = file_type.split('/')[-1]
                                if mime_type == 'quicktime':
                                    file_ext = '.mov'
                                elif mime_type in ['mp4', 'avi', 'webm', 'mkv', 'flv', 'wmv']:
                                    file_ext = f'.{mime_type}'
                                else:
                                    file_ext = '.mp4'
                            else:
                                file_ext = os.path.splitext(original_filename)[1] if original_filename else '.mp4'
                            
                            # 处理arrayBuffer数据
                            array_buffer_data = video_file['data']
                            if isinstance(array_buffer_data, (list, tuple)):
                                # arrayBuffer可能被转换为数组
                                video_data = bytes(array_buffer_data)
                            elif isinstance(array_buffer_data, bytes):
                                video_data = array_buffer_data
                            elif isinstance(array_buffer_data, str):
                                # 可能是base64编码的数据
                                try:
                                    video_data = base64.b64decode(array_buffer_data)
                                except:
                                    logger.error(f"❌ 无法解码视频数据")
                                    continue
                            else:
                                logger.error(f"❌ 不支持的arrayBuffer数据类型: {type(array_buffer_data)}")
                                continue
                            
                            logger.info(f"🎥 客户端 {client_id} 视频处理成功 (对象格式): {original_filename}, 类型: {file_type}, 大小: {file_size}字节")
                        elif isinstance(video_file, bytes):
                            # 这是bytes数据
                            video_data = video_file
                            logger.info(f"🎥 客户端 {client_id} 视频处理成功 (bytes): {original_filename}")
                        elif isinstance(video_file, str):
                            # 这可能是base64数据
                            try:
                                base64_data = re.sub('^data:video/.+;base64,', '', video_file)
                                video_data = base64.b64decode(base64_data)
                                logger.info(f"🎥 客户端 {client_id} 视频处理成功 (base64): {original_filename}")
                            except Exception as e:
                                logger.error(f"❌ base64视频解码失败: {e}")
                                continue
                        else:
                            logger.warning(f"⚠️ 不支持的视频格式: {type(video_file)}")
                            logger.info(f"🔍 视频数据内容预览: {str(video_file)[:200]}...")
                            continue
                        
                        if video_data:
                            # 生成唯一文件名：client_id_随机字符串_索引.扩展名
                            filename = f"{client_id}_{uuid.uuid4().hex[:8]}_{i}{file_ext}"
                            filepath = os.path.join(TEMP_DIR, filename)
                            
                            # 保存视频文件
                            with open(filepath, 'wb') as f:
                                f.write(video_data)
                            
                            processed_videos.append(filepath)
                            logger.info(f"🎥 客户端 {client_id} 视频保存成功: {filename} (原文件: {original_filename})")
                            
                    except Exception as e:
                        logger.error(f"❌ 视频处理失败: {e}")
                        continue
            
            # 记录处理结果
            if processed_images or processed_videos:
                logger.info(f"📦 客户端 {client_id} stream处理完成: {len(processed_images)}张图片, {len(processed_videos)}个视频")
            
            # 把image和video添加到队列
            new_queries = {
                'prompt': message,
                'images': processed_images,
                'videos': processed_videos,
                'stop_offline_generate': False,
                # 把params的内容放进来
            }
            if params:
                new_queries.update(params)
            logger.info(f"📦 客户端 {client_id} 上报参数：{new_queries}")
            
            # 这里可以添加将new_queries发送到模型处理队列的逻辑
            manager.add_offline_data(new_queries)
            
    except Exception as e:
        logger.error(f"❌ 处理数据失败: {e}")
        socketio.emit('error', {'message': f'数据处理失败: {str(e)}'}, room=client_id)


@socketio.on('pause_offline_output')
def handle_pause_offline_generate():
    """处理暂停离线理解请求"""
    client_id = request.sid
    manager = client_managers.get(client_id)
    if manager is None:
        return
    manager.pause_offline_generate()

if __name__ == '__main__':
    logger.info("🚀 启动VideoLLM后端服务...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
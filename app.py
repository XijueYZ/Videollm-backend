import re
import os
import shutil
import uuid
from flask import Flask, request, jsonify, send_file
from flask_socketio import SocketIO
from flask_cors import CORS  # 添加这行
from threading import Lock
import logging      
import time
import threading
from model_pool import ModelPool
from config import Config
import base64
from io import BytesIO
from PIL import Image
from database import db, init_database, create_conversation, get_conversation, get_conversations, get_conversation_messages, add_message_to_conversation, update_conversation_title, delete_conversation
import json

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)

# 添加CORS支持
CORS(app, origins="*", allow_headers=["Content-Type", "Authorization"], supports_credentials=True)

app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

# 初始化数据库
init_database(app)

# 创建保存图片和视频的文件夹
# 放在当前文件夹的父级文件夹下
TEMP_DIR = os.path.join(os.path.dirname(os.getcwd()), 'temp_uploads')
os.makedirs(TEMP_DIR, exist_ok=True)
logger.info(f"📁 TEMP_DIR路径: {TEMP_DIR}")

# 添加静态文件服务
@app.route('/temp_uploads/<path:filename>')
def serve_temp_file(filename):
    """提供临时文件访问"""
    try:
        file_path = os.path.join(TEMP_DIR, filename)
        logger.info(f"🔍 请求临时文件: {filename}")
        logger.info(f"🔍 完整路径: {file_path}")
        logger.info(f"🔍 文件是否存在: {os.path.exists(file_path)}")
        
        if not os.path.exists(file_path):
            logger.warning(f"❌ 临时文件不存在: {file_path}")
            return jsonify({'error': '文件不存在'}), 404
        
        # 根据文件扩展名设置正确的MIME类型
        if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
            return send_file(file_path, mimetype='image/*')
        elif filename.lower().endswith(('.mp4', '.webm', '.ogg')):
            return send_file(file_path, mimetype='video/*')
        else:
            return jsonify({'error': '不支持的文件类型'}), 400
    except Exception as e:
        logger.error(f"获取临时文件失败: {e}")
        return jsonify({'error': str(e)}), 500

# def cleanup_temp_files(client_id=None, max_age_hours=24):
#     """清理临时文件"""
#     try:
#         current_time = time.time()
#         for filename in os.listdir(TEMP_DIR):
#             filepath = os.path.join(TEMP_DIR, filename)
            
#             # 如果指定了client_id，只清理该客户端的文件
#             if client_id and not filename.startswith(f"{client_id}_"):
#                 continue
                
#             # 检查文件年龄
#             file_age = current_time - os.path.getctime(filepath)
#             if file_age > max_age_hours * 3600:  # 转换为秒
#                 os.remove(filepath)
#                 logger.info(f"🗑️ 清理过期临时文件: {filename}")
                
#     except Exception as e:
#         logger.error(f"❌ 清理临时文件失败: {e}")

# 启动定期清理任务
# def start_cleanup_task():
#     """启动定期清理任务"""
#     def cleanup_worker():
#         while True:
#             time.sleep(3600)  # 每小时清理一次
#             cleanup_temp_files()
    
#     threading.Thread(target=cleanup_worker, daemon=True).start()

# start_cleanup_task()

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

@app.route('/api/upload-video', methods=['POST'])
def upload_video():
    """处理视频文件上传"""
    try:
        logger.info('视频上传开始')
        client_id = request.form.get('socket_id')
        # 检查是否有文件上传
        if 'video' not in request.files:
            return jsonify({'error': '没有找到视频文件'}), 400
        
        video_file = request.files['video']
        if video_file.filename == '':
            return jsonify({'error': '没有选择文件'}), 400
        
        # 获取文件信息
        original_filename = request.form.get('filename', video_file.filename)
        file_size = request.form.get('filesize', 0)
        
        # 验证文件类型
        if not video_file.content_type or not video_file.content_type.startswith('video/'):
            return jsonify({'error': '文件类型不是视频格式'}), 400
        
        # 生成文件夹：客户端_日期
        folder_name = f"{client_id}_{int(time.time())}"
        os.makedirs(os.path.join(TEMP_DIR, folder_name), exist_ok=True)
        # 生成唯一文件名
        file_ext = os.path.splitext(original_filename)[1] if original_filename else '.mp4'
        unique_filename = f"video_{uuid.uuid4()}{file_ext}"
        file_path = os.path.join(TEMP_DIR, folder_name, unique_filename)
        
        # 保存文件
        video_file.save(file_path)
        
        # 验证文件是否保存成功
        if not os.path.exists(file_path):
            return jsonify({'error': '文件保存失败'}), 500
        
        saved_size = os.path.getsize(file_path)
        logger.info(f"🎥 视频上传成功: {original_filename} -> {unique_filename}, 大小: {saved_size / 1024 / 1024:.2f} MB")
        
        return jsonify({
            'success': True,
            'filePath': file_path,
            'path': file_path,  # 兼容不同的字段名
            'originalName': original_filename,
            'savedName': unique_filename,
            'size': saved_size,
            'message': '视频上传成功'
        })
        
    except Exception as e:
        logger.error(f"❌ 视频上传失败: {e}")
        return jsonify({'error': f'上传失败: {str(e)}'}), 500
 

@app.route('/api/conversations', methods=['GET'])
def get_conversations_api():
    """获取对话列表"""
    try:
        conversation_type = request.args.get('type')  # 'chat' 或 'stream'
        conversations = get_conversations(conversation_type)
        return jsonify({
            'success': True,
            'conversations': conversations
        })
    except Exception as e:
        logger.error(f"获取对话列表失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/conversations/<conversation_id>/messages', methods=['GET'])
def get_conversation_messages_api(conversation_id):
    """获取对话消息"""
    try:
        messages = get_conversation_messages(conversation_id)
        return jsonify({
            'success': True,
            'messages': messages
        })
    except Exception as e:
        logger.error(f"获取对话消息失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/conversations', methods=['POST'])
def create_conversation_api():
    """ 创建新对话 """
    """ title: 对话标题, type: 对话类型, conversationId: 对话ID"""
    try:
        data = request.get_json()
        title = data.get('title', '新对话')
        conversation_type = data.get('type', 'chat')
        
        conversation_id = create_conversation(conversation_id=str(uuid.uuid4()), title=title, conversation_type=conversation_type)
        # 
        
        return jsonify({
            'success': True,
            'conversationId': conversation_id
        })
    except Exception as e:
        logger.error(f"创建对话失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/conversations/<conversation_id>/title', methods=['PUT'])
def update_conversation_title_api(conversation_id):
    """更新对话标题"""
    try:
        data = request.get_json()
        title = data.get('title')
        
        if not title:
            return jsonify({'success': False, 'error': '标题不能为空'}), 400
        
        update_conversation_title(app, conversation_id, title)
        
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"更新对话标题失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/conversations/<conversation_id>/content', methods=['PUT'])
def update_conversation_content_api(conversation_id):
    """更新对话内容"""
    """
    is_user: 是否是用户消息
    is_error: 是否是错误消息
    content: 消息内容
    """
    try:
        data = request.get_json()
        is_user = data.get('is_user')
        is_error = data.get('is_error')
        content = data.get('content')
        
        if not content:
            return jsonify({'success': False, 'error': '内容不能为空'}), 400
        
        add_message_to_conversation(app, conversation_id, 'user' if is_user else 'robot', content, is_error)
        
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"更新对话标题失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/conversations/<conversation_id>', methods=['DELETE'])
def delete_conversation_api(conversation_id):
    """删除对话"""
    try:
        logger.info(f"删除对话: {conversation_id}")
        # 先把对话列表中相关的文件删除，再删除对话
        messages = get_conversation_messages(conversation_id)
        for message in messages:
            if message.get('files'):
                files_info = json.loads(message['files'])
                if 'images' in files_info:
                    for img in files_info['images']:
                        os.remove(img['path'])
                if 'videos' in files_info:
                    for vid in files_info['videos']:
                        os.remove(vid['path'])
        # NOTE: 理论上这里应该把变成空了的文件夹删掉，但是考虑到没有快捷有效的方式，而遍历可能更加耗时，空文件夹又没有影响，所以暂时没有做
        delete_conversation(app, conversation_id)
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"删除对话失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/files/<path:filename>')
def get_file(filename):
    """获取保存的媒体文件"""
    try:
        file_path = os.path.join(TEMP_DIR, filename)
        if not os.path.exists(file_path):
            return jsonify({'error': '文件不存在'}), 404
        
        # 根据文件扩展名设置正确的MIME类型
        if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
            
            return send_file(file_path, mimetype='image/*')
        elif filename.lower().endswith(('.mp4', '.webm', '.ogg')):
            return send_file(file_path, mimetype='video/*')
        else:
            return jsonify({'error': '不支持的文件类型'}), 400
    except Exception as e:
        logger.error(f"获取文件失败: {e}")
        return jsonify({'error': str(e)}), 500

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
def handle_disconnect(reason):
    """处理客户端断连"""
    client_id = request.sid
    logger.info(f"❌ 客户端 {client_id} 断开连接，原因是：{reason}")
    
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
    """
    conversationId: 对话ID
    type: 请求类型
    message: 文本消息
    image_data: 图片数据(实时场景)
    images: 图片列表（离线场景）
    videos: 视频列表（离线场景）
    params: 参数
    """
    client_id = request.sid
    logger.info(f"📨 收到客户端 {client_id} 的 send_data 请求")
    # 从请求中获取conversation_id
    conversation_id = data.get('conversationId')
    if not conversation_id:
        socketio.emit('error', {'message': '缺少conversationId参数'}, room=client_id)
        return

     # 验证对话是否存在
    conversation = get_conversation(conversation_id)
    if not conversation:
        socketio.emit('error', {'message': f'对话 {conversation_id} 不存在'}, room=client_id)
        return
    
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
                add_message_to_conversation(app, conversation_id, 'user', message, False, None)
            if image:
                manager.add_image(image)
        
        elif send_type == 'chat':
            processed_images = []
            processed_videos = []
            
            # 把image从File转化为Image并保存到临时文件夹
            saved_image_paths = []
            if images:
                for i, img_file in enumerate(images):
                    try:
                        image = None
                        filename = f"image_{uuid.uuid4()}_{i}"
                        
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
                            # 保存图片到临时文件夹
                            try:
                                # 生成文件夹：客户端_日期
                                folder_name = f"{client_id}_{int(time.time())}"
                                os.makedirs(os.path.join(TEMP_DIR, folder_name), exist_ok=True)
                                # 生成唯一的文件名
                                file_ext = '.png'  # 默认使用PNG格式保存
                                unique_filename = f"image_{uuid.uuid4()}_{i}{file_ext}"
                                file_path = os.path.join(TEMP_DIR, folder_name, unique_filename)
                                
                                # 保存图片
                                image.save(file_path)
                                saved_image_paths.append({
                                    'path': file_path,
                                    'name': filename
                                })
                                logger.info(f"🖼️ 图片已保存: {file_path}")
                            except Exception as e:
                                logger.error(f"❌ 保存图片失败: {e}")
                                continue
                            
                            # 添加到processed_images用于模型处理
                            processed_images.append(image)
                            
                    except Exception as e:
                        logger.error(f"❌ 图片处理失败: {e}")
                        continue
            
            # 处理视频路径（前端通过HTTP接口上传后发送路径）
            if videos:
                for i, video_item in enumerate(videos):
                    try:
                        if isinstance(video_item, dict) and 'path' in video_item:
                            # 这是包含路径的对象格式: {path: '/path/to/video', name: 'xxx'}
                            video_path = video_item['path']
                            original_name = video_item.get('name', f'video_{i}')
                            if os.path.exists(video_path):
                                processed_videos.append(video_path)
                                logger.info(f"🎥 客户端 {client_id} 视频路径已确认: {video_path} (原名: {original_name})")
                            else:
                                logger.error(f"❌ 视频文件不存在: {video_path}")
                                continue  
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
            
            # 保存用户消息到数据库
            files_info = None
            if saved_image_paths or processed_videos:
                files_info = json.dumps({
                    'images': [{
                        'path': img_info['path'],
                        'name': img_info['name']
                    } for img_info in saved_image_paths],
                    'videos': [{
                        'path': video_path,
                        'name': os.path.basename(video_path)
                    } for video_path in processed_videos]
                })
           
            if message or processed_images or processed_videos:
                add_message_to_conversation(app, conversation_id, 'user', message, False, files_info)
            
    except Exception as e:
        logger.error(f"❌ 处理数据失败: {e}")
        # 保存错误消息
        add_message_to_conversation(app, conversation_id, 'robot', f'数据处理失败: {str(e)}', True)
        socketio.emit('error', {'message': f'数据处理失败: {str(e)}'}, room=client_id)


@socketio.on('pause_offline_output')
def handle_pause_offline_generate():
    """处理暂停离线理解请求"""
    client_id = request.sid
    manager = client_managers.get(client_id)
    if manager is None:
        return
    manager.pause_offline_generate()

# 在现有代码基础上添加这个新的HTTP接口
@app.route('/api/allocate-model', methods=['POST'])
def allocate_model():
    """处理模型分配请求 - 新的独立事件"""
    """
    activeKey: 客户端的activeKey
    conversationId: 对话ID
    sid: 客户端Socket ID
    """
    try:
        data = request.get_json()
        # 从请求中获取socket_id
        client_id = data.get('sid', None)
        
        logger.info(f"📋 客户端 {client_id} 请求模型分配：{data}")
        
        if not client_id:
            return jsonify({'error': '缺少socket_id参数'}), 400
            
        # 获取activeKey，如果没有传递则使用默认值
        new_active_key = data.get('activeKey', 'None') if isinstance(data, dict) else 'None'
        
        if new_active_key == 'None':
            return
            
        manager_to_release = None
        with client_lock:
            if client_id in client_managers:
                manager_to_release = client_managers.pop(client_id) 
        
        if manager_to_release:  
            try:
                model_pool.release_model(manager_to_release)
                logger.info(f"🔄 因activeKey变化释放管理器 {manager_to_release.manager_id}")
                
            except Exception as e:
                logger.error(f"❌ 释放管理器失败: {e}")

        try:
            with app.app_context():
                
                # 尝试获取模型 - 可以设置更长的超时时间
                manager = model_pool.acquire_model(timeout=30)  # 30秒超时
                
                if manager is None:
                    return jsonify({'success': False, 'error': '模型池繁忙，请稍后重试'})
                
                # 从请求中获取conversation_id
                conversation_id = data.get('conversationId')
                if not conversation_id:
                    return jsonify({'success': False, 'error': '缺少conversationId参数'})
                
                # 验证对话是否存在
                conversation = get_conversation(conversation_id)
                if not conversation:
                    return jsonify({'success': False, 'error': f'对话 {conversation_id} 不存在'})
                
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
            
                return jsonify({
                    'success': True,
                    'modelId': manager.manager_id,
                    'conversationId': conversation_id
                })
                
        except Exception as e:
            logger.error(f"❌ 为客户端 {client_id} 分配模型失败: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500
            
        
    except Exception as e:
        logger.error(f"❌ 分配模型失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/release-model', methods=['POST'])
def release_model():
    """HTTP接口释放模型"""
    try:
        data = request.get_json()
        sid = data.get('sid')
        
        if not sid:
            return jsonify({'success': False, 'error': '缺少sid参数'}), 400
        
        # 查找并释放模型
        with client_lock:
            manager = client_managers.pop(sid, None)
        
        if manager:
            try:
                model_pool.release_model(manager)
                logger.info(f"✅ 客户端 {sid} 的模型已释放")
                return jsonify({'success': True})
            except Exception as e:
                logger.error(f"❌ 释放模型失败: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
        else:
            return jsonify({'success': False, 'error': '未找到对应的模型'}), 404
            
    except Exception as e:
        logger.error(f"❌ 释放模型失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("🚀 启动VideoLLM后端服务...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
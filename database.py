import os
import json
from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy
import uuid
import logging  

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化数据库
db = SQLAlchemy()

class Conversation(db.Model):
    """对话会话表"""
    __tablename__ = 'videollm_conversations'
    
    id = db.Column(db.String(36), primary_key=True)  # UUID
    title = db.Column(db.String(200), nullable=False)  # 对话标题
    conversation_type = db.Column(db.String(20), nullable=False)  # 'chat' 或 'stream'
    messages_data = db.Column(db.Text, default='{"data": []}')  # JSON字符串存储消息
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    def get_messages(self):
        """获取消息列表"""
        try:
            data = json.loads(self.messages_data)
            return data.get('data', [])
        except (json.JSONDecodeError, TypeError):
            return []
    
    def add_message(self, message_type: str, content: str, is_error: bool = False, files_info: str = None):
        """添加消息到对话"""
        messages = self.get_messages()
        
        message = {
            'id': str(uuid.uuid4()),
            'type': message_type,  # 'user' 或 'robot'
            'content': content,
            'isError': is_error,
            'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
            'files': files_info
        }
        
        messages.append(message)
        
        # 更新消息数据
        self.messages_data = json.dumps({'data': messages}, ensure_ascii=False)
        self.updated_at = datetime.now(timezone.utc)

        db.session.commit()
        logger.info(f"✅ 消息添加成功: {message}")
        
        return message['id']
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            'id': self.id,
            'title': self.title,
            'type': self.conversation_type,
            'createdAt': int(self.created_at.timestamp() * 1000),
            'updatedAt': int(self.updated_at.timestamp() * 1000),
            'messageCount': len(self.get_messages())
        }

def init_database(app):
    """初始化数据库"""
    # 设置数据库路径
    db_path = os.path.join(os.getcwd(), 'videollm.db')
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # 初始化数据库
    db.init_app(app)
    
    # 创建表
    with app.app_context():
        db.create_all()
        print(f"✅ 数据库初始化完成: {db_path}")

def create_conversation(conversation_id: str, title: str, conversation_type: str) -> str:
    """创建新对话"""
    conversation = Conversation(
        id=conversation_id,
        title=title,
        conversation_type=conversation_type
    )
    
    db.session.add(conversation)
    db.session.commit()
    
    return conversation_id

def get_conversation(conversation_id: str) -> Conversation:
    """获取指定对话"""
    return Conversation.query.get(conversation_id)

def get_conversations(conversation_type: str = None, limit: int = 50) -> list:
    """获取对话列表"""
    query = Conversation.query
    
    if conversation_type:
        query = query.filter(Conversation.conversation_type == conversation_type)
    
    conversations = query.order_by(Conversation.updated_at.desc()).limit(limit).all()
    return [conv.to_dict() for conv in conversations]

def get_conversation_messages(conversation_id: str) -> list:
    """获取对话的所有消息"""
    conversation = Conversation.query.get(conversation_id)
    if not conversation:
        return []
    
    return conversation.get_messages()

def add_message_to_conversation(app, conversation_id: str, message_type: str, content: str, is_error: bool = False, files_info: str = None) -> str:
    """添加消息到指定对话"""
    with app.app_context():
        conversation = Conversation.query.get(conversation_id)
        if not conversation:
            raise ValueError(f"对话 {conversation_id} 不存在")

        return conversation.add_message(message_type, content, is_error, files_info)

def update_conversation_title(app, conversation_id: str, title: str):
    """更新对话标题"""
    with app.app_context():
        conversation = Conversation.query.get(conversation_id)
        if conversation:
            conversation.title = title
            conversation.updated_at = datetime.now(timezone.utc)
            db.session.commit()

def delete_conversation(app, conversation_id: str):
    """删除对话"""
    with app.app_context():
        conversation = Conversation.query.get(conversation_id)
        if conversation:
            db.session.delete(conversation)
            db.session.commit()

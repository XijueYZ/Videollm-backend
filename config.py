import os


class Config:
    """应用配置类"""
    
    # Flask配置
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'videollm-secret-key-2024'
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    # SocketIO配置
    SOCKETIO_ASYNC_MODE = 'threading'
    SOCKETIO_CORS_ALLOWED_ORIGINS = "*"
    
    # 模型池配置
    MODEL_POOL_SIZE = int(os.environ.get('MODEL_POOL_SIZE', 8))
    MODEL_ACQUIRE_TIMEOUT = float(os.environ.get('MODEL_ACQUIRE_TIMEOUT', 0.1))
    
    # 服务器配置
    HOST = os.environ.get('HOST', '0.0.0.0')
    PORT = int(os.environ.get('PORT', 5000))
    
    # 日志配置
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    
    # 模型配置（可以根据实际模型需求调整）
    MODEL_CONFIG = {
        'max_tokens': int(os.environ.get('MAX_TOKENS', 2048)),
        'temperature': float(os.environ.get('TEMPERATURE', 0.7)),
        'model_path': os.environ.get('MODEL_PATH', './models/'),
    }


class DevelopmentConfig(Config):
    """开发环境配置"""
    DEBUG = True
    LOG_LEVEL = 'DEBUG'


class ProductionConfig(Config):
    """生产环境配置"""
    DEBUG = False
    LOG_LEVEL = 'WARNING'


class TestingConfig(Config):
    """测试环境配置"""
    TESTING = True
    MODEL_POOL_SIZE = 2  # 测试时使用较小的模型池


# 配置字典
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
} 
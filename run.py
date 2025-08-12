#!/usr/bin/env python3
"""
VideoLLM Backend 启动脚本
"""

import os
import sys
import logging
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app, socketio, model_pool, logger


def setup_logging():
    """设置日志配置"""
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    
    # 设置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    # 添加处理器到根日志器
    if not root_logger.handlers:
        root_logger.addHandler(console_handler)


def main():
    """主函数"""
    setup_logging()
    
    # 获取配置
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    logger.info("=" * 50)
    logger.info("VideoLLM Backend Server 启动中...")
    logger.info(f"服务地址: http://{host}:{port}")
    logger.info(f"调试模式: {debug}")
    logger.info(f"模型池大小: {model_pool.total_models}")
    logger.info("=" * 50)
    
    try:
        # 启动服务器
        socketio.run(
            app,
            host=host,
            port=port,
            debug=debug,
            allow_unsafe_werkzeug=True
        )
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭服务器...")
    except Exception as e:
        logger.error(f"服务器启动失败: {e}")
        sys.exit(1)
    finally:
        # 清理资源
        logger.info("正在清理资源...")
        model_pool.shutdown()
        logger.info("服务器已关闭")


if __name__ == '__main__':
    main() 
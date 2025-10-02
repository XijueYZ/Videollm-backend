# 用于测试模型异常中断后的重置功能

import random
import time
from model_pool import ModelManager

class ExceptionInjector:
    """异常注入器，用于测试异常恢复机制"""
    
    def __init__(self, manager: ModelManager):
        self.manager = manager
        self.original_methods = {}
        self.injection_enabled = False
    
    def inject_exception(self, method_name: str, exception: Exception, probability: float = 0.1):
        """注入异常到指定方法"""
        if hasattr(self.manager.model_instance, method_name):
            original_method = getattr(self.manager.model_instance, method_name)
            self.original_methods[method_name] = original_method
            
            def exception_wrapper(*args, **kwargs):
                if self.injection_enabled and random.random() < probability:
                    print(f"注入异常到 {method_name}: {exception}")
                    self.manager._handle_model_exception(exception)
                    raise exception
                return original_method(*args, **kwargs)
            
            setattr(self.manager.model_instance, method_name, exception_wrapper)
    
    def enable_injection(self):
        """启用异常注入"""
        self.injection_enabled = True
        print("异常注入已启用")
    
    def disable_injection(self):
        """禁用异常注入"""
        self.injection_enabled = False
        print("异常注入已禁用")
    
    def restore_original_methods(self):
        """恢复原始方法"""
        for method_name, original_method in self.original_methods.items():
            setattr(self.manager.model_instance, method_name, original_method)
        self.original_methods.clear()
        print("已恢复原始方法")

def test_with_exception_injection():
    """使用异常注入进行测试"""
    # 创建模型管理器
    manager = ModelManager("test_manager", frame_extract_num_threads=1, gpu_id=0)
    
    # 创建异常注入器
    injector = ExceptionInjector(manager)
    
    # 注入各种异常
    injector.inject_exception("real_time_generate", RuntimeError("GPU内存不足"), 0.3)
    injector.inject_exception("offline_generate", ValueError("输入数据格式错误"), 0.2)
    
    # 启用异常注入
    injector.enable_injection()
    
    try:
        # 启动会话
        token_callback = lambda x: print(f"收到token: {x}")
        manager.start_session(token_callback, "stream")
        
        # 发送一些数据
        for i in range(100):
            manager.add_prompt(f"测试prompt {i}")
            time.sleep(0.5)
            
            # 检查健康状态
            if not manager.is_healthy():
                print(f"模型在第{i}次请求后变为不健康")
                break
        
        # 尝试重置
        if not manager.is_healthy():
            print("尝试重置模型...")
            manager.reset_model()
            print(f"重置后健康状态: {manager.is_healthy()}")
        
    finally:
        # 清理
        injector.disable_injection()
        injector.restore_original_methods()
        manager.stop_session()

if __name__ == "__main__":
    test_with_exception_injection()
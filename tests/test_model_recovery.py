# 用于测试模型异常中断后的重置功能

import pytest
import time
from unittest.mock import Mock, patch
from model_pool import ModelManager, ModelPool

class TestModelRecovery:
    """测试模型异常恢复功能"""
    
    def test_model_exception_handling(self):
        """测试模型异常处理"""
        # 创建一个模拟的ModelManager
        manager = ModelManager("test_manager", frame_extract_num_threads=1, gpu_id=0)
        
        # 模拟模型实例
        mock_model = Mock()
        mock_processor = Mock()
        manager.model_instance = mock_model
        manager.processor = mock_processor
        
        # 模拟模型方法抛出异常
        mock_model.real_time_generate.side_effect = RuntimeError("模拟模型异常")
        
        # 启动会话
        token_callback = Mock()
        manager.start_session(token_callback, "stream")
        
        # 添加一些数据触发异常
        manager.add_prompt("测试prompt")
        manager.add_image("测试图片")
        
        # 等待异常处理
        time.sleep(1)
        
        # 验证异常被正确处理
        assert not manager.is_healthy()
        
        # 停止会话
        manager.stop_session()
    
    def test_model_reset_functionality(self):
        """测试模型重置功能"""
        manager = ModelManager("test_manager", frame_extract_num_threads=1, gpu_id=0)
        
        # 模拟模型不健康状态
        manager._is_healthy = False
        
        # 重置模型
        manager.reset_model()
        
        # 验证模型被重置为健康状态
        assert manager.is_healthy()
    
    def test_pool_acquire_with_unhealthy_manager(self):
        """测试获取不健康管理器时的处理"""
        pool = ModelPool(pool_size=1)
        
        # 获取管理器并标记为不健康
        manager = pool.acquire_model()
        assert manager is not None
        
        manager._is_healthy = False
        
        # 释放管理器
        pool.release_model(manager)
        
        # 再次获取管理器，应该自动重置
        manager2 = pool.acquire_model()
        assert manager2 is not None
        assert manager2.is_healthy()
    
    def test_forced_model_failure_simulation(self):
        """模拟强制模型失败场景"""
        # 创建一个会失败的模型管理器
        with patch('model_pool.AutoModelForCausalLM.from_pretrained') as mock_model_load:
            # 第一次加载成功，第二次失败
            mock_model_load.side_effect = [
                Mock(),  # 第一次成功
                RuntimeError("GPU内存不足"),  # 第二次失败
                Mock()   # 第三次成功
            ]
            
            manager = ModelManager("test_manager", frame_extract_num_threads=1, gpu_id=0)
            assert manager.is_healthy()
            
            # 模拟异常
            manager._handle_model_exception(RuntimeError("模拟异常"))
            assert not manager.is_healthy()
            
            # 尝试重置
            try:
                manager.reset_model()
                # 如果重置成功，验证状态
                assert manager.is_healthy()
            except Exception as e:
                # 如果重置失败，验证异常被正确处理
                assert not manager.is_healthy()
                print(f"重置失败（预期）: {e}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
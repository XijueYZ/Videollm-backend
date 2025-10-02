import threading
import time
import logging
from typing import Optional, List, Callable, Any
from queue import Queue, Empty
import uuid

import torch
from transformers import AutoProcessor, AutoModelForCausalLM

logger = logging.getLogger(__name__)


class ModelManager:
    """模型管理器，负责管理单个模型实例的完整生命周期"""
    
    def __init__(self, manager_id: str, frame_extract_num_threads = 1, gpu_id = None):
        checkpoint = "/inspire/hdd/project/embodied-multimodality/public/pywang/jihuai/real_time_chat/models/0913_1_1_1_w_1_0"
        self.processor = AutoProcessor.from_pretrained(
            checkpoint,
            trust_remote_code=True,
            frame_extract_num_threads=frame_extract_num_threads
        )
            
        self.model_instance = AutoModelForCausalLM.from_pretrained(
            checkpoint,
            trust_remote_code=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="flash_attention_2",
            device_map={"": f"cuda:{gpu_id}"}  # 确保整个模型都在指定GPU上
        )
        # self.processor = AutoProcessor.from_pretrained(checkpoint, trust_remote_code=True, frame_extract_num_threads=1)

        # self.model_instance = AutoModelForCausalLM.from_pretrained(checkpoint, trust_remote_code=True, device_map="auto", torch_dtype=torch.bfloat16, attn_implementation="flash_attention_2")

        self.manager_id = manager_id
        self.is_active = False
        self.created_at = time.time()
        self.last_used = time.time()
        
        # 两个实时视频入参队列
        self.prompt_queue = Queue()  # 存放前端发送的prompt
        self.image_queue = Queue()   # 存放前端发送的图片

        # 一个离线入参队列
        self.offline_data_queue = Queue()

        self.token_queue = Queue()   # 存放模型生成的token

        # 线程控制
        self._generate_thread = None
        self._monitor_thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        
        # 回调函数
        self._token_callback = None

        self.type = "chat"
        
        self.is_stop = True # 标识real_time_generate是否已停止
        
        logger.info(f"创建模型管理器: {manager_id}")

    def start_session(self, token_callback: Callable[[str], None], new_active_key: str) -> bool:
        """
        启动模型会话
        """
        if new_active_key == "chat":
            self.type = "chat"
            return self.start_offline_session(token_callback)
        else:
            self.type = "stream"
            return self.start_real_time_session(token_callback)
    
    def start_offline_session(self, token_callback: Callable[[str], None]) -> bool:
        """
        启动离线理解会话
        
        Args:
            token_callback: 接收新token的回调函数
            
        Returns:
            是否成功启动
            
        Raises:
            Exception: 当模型启动失败时抛出异常
        """
        with self._lock:
            if self.is_active:
                error_msg = f"管理器 {self.manager_id} 已处于活跃状态"
                logger.warning(error_msg)
                raise RuntimeError(error_msg)
            
            self.is_active = True
            self._token_callback = token_callback
            self._stop_event.clear()
            
            # 启动生成线程
            self._generate_thread = threading.Thread(
                target=self._offline_generation_loop,
                name=f"Generate-{self.manager_id}",
                daemon=True
            )
            
            # 启动监听线程
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name=f"Monitor-{self.manager_id}",
                daemon=True
            )
            
            try:
                self._generate_thread.start()
                self._monitor_thread.start()
                
                logger.info(f"模型管理器 {self.manager_id} 会话已启动")
                return True
                
            except Exception as e:
                error_msg = f"启动模型管理器 {self.manager_id} 失败: {e}"
                logger.error(error_msg)
                self.is_active = False
                raise RuntimeError(error_msg)
 
    
    def start_real_time_session(self, token_callback: Callable[[str], None]) -> bool:
        """
        启动模型会话
        
        Args:
            token_callback: 接收新token的回调函数
            
        Returns:
            是否成功启动
            
        Raises:
            Exception: 当模型启动失败时抛出异常
        """
        with self._lock:
            if self.is_active:
                error_msg = f"管理器 {self.manager_id} 已处于活跃状态"
                logger.warning(error_msg)
                raise RuntimeError(error_msg)
            
            self.is_active = True
            self._token_callback = token_callback
            self._stop_event.clear()
            
            # 启动生成线程
            self._generate_thread = threading.Thread(
                target=self._generation_loop,
                name=f"Generate-{self.manager_id}",
                daemon=True
            )
            
            # 启动监听线程
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name=f"Monitor-{self.manager_id}",
                daemon=True
            )
            
            try:
                self._generate_thread.start()
                self._monitor_thread.start()
                
                logger.info(f"模型管理器 {self.manager_id} 会话已启动")
                return True
                
            except Exception as e:
                error_msg = f"启动模型管理器 {self.manager_id} 失败: {e}"
                logger.error(error_msg)
                self.is_active = False
                raise RuntimeError(error_msg)
    
    def _offline_generation_loop(self):
        """离线理解循环线程，负责调用模型的offline_generate函数"""
        logger.info(f"管理器 {self.manager_id} 离线理解线程启动")
        # 标记开始理解
        self.is_stop = False
        try:
            # 调用模型的offline_generate函数，只调用一次
            if hasattr(self.model_instance, 'offline_generate'):
                logger.info(f"管理器 {self.manager_id} 开始调用offline_generate函数")
                self.model_instance.offline_generate(self.processor, self.offline_data_queue, self.token_queue)
            else:
                error_msg = f"模型实例不支持offline_generate方法"
                logger.error(error_msg)
                raise AttributeError(error_msg)
        except Exception as e:
            logger.error(f"管理器 {self.manager_id} 离线理解线程出错: {e}")
            # 不再向token_queue放入错误信息，而是重新抛出异常
            raise e
        finally:
            # 标记理解结束
            self.is_stop = True
            logger.info(f"管理器 {self.manager_id} 离线理解线程结束，已标记为停止")
    
    def pause_offline_generate(self):
        """暂停离线理解"""
        logger.info(f"管理器 {self.manager_id} 暂停离线理解")
        self.model_instance.stop_real_time_generate()

    def _generation_loop(self):
        """生成循环线程，负责调用模型的generate函数"""
        logger.info(f"管理器 {self.manager_id} 生成线程启动")
        # 标记开始生成
        self.is_stop = False
        try:
            # 调用模型的generate函数，只调用一次
            # 函数内部会自己循环处理队列
            if hasattr(self.model_instance, 'real_time_generate'):
                logger.info(f"管理器 {self.manager_id} 开始调用generate函数")
                self.model_instance.real_time_generate(self.image_queue, self.prompt_queue, self.token_queue, self.processor, max_tokens_per_turn=86400, do_sample=True)
            else:
                error_msg = f"模型实例不支持generate方法"
                logger.error(error_msg)
                raise AttributeError(error_msg)
                
        except Exception as e:
            logger.error(f"管理器 {self.manager_id} 生成线程出错: {e}")
            # 不再向token_queue放入错误信息，而是重新抛出异常
            raise e
        finally:
            # 标记生成结束
            self.is_stop = True
            logger.info(f"管理器 {self.manager_id} 生成线程结束，已标记为停止")
            
    def _monitor_loop(self):
        """监听循环线程，负责监听token队列并回调前端"""
        logger.info(f"管理器 {self.manager_id} 监听线程启动")
        
        while not self._stop_event.is_set():
            try:
                # 监听token队列
                try:
                    token = self.token_queue.get(timeout=0.1)
                    
                    # 回调前端
                    if self._token_callback:
                        self._token_callback(token)
                    
                    logger.info(f"管理器 {self.manager_id} 输出token: {token}")
                    
                    # 检查是否是结束标记
                    if token == "[DONE]" or token == "[ERROR]":
                        logger.info(f"管理器 {self.manager_id} 收到结束标记: {token}")
                        
                except Empty:
                    continue
                    
            except Exception as e:
                logger.error(f"管理器 {self.manager_id} 监听循环出错: {e}")
        
        logger.info(f"管理器 {self.manager_id} 监听线程已停止")
     
    def add_prompt(self, prompt: str):
        """添加prompt到队列"""
        if not self.is_active:
            error_msg = f"管理器 {self.manager_id} 未启动"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        self.prompt_queue.put(prompt)
        logger.info(f"管理器 {self.manager_id} 接收prompt: {prompt[:50]}...")
    
    def add_image(self, image: Any):
        """实时模式下，添加图片到队列"""
        if not self.is_active:
            error_msg = f"管理器 {self.manager_id} 未启动"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        self.image_queue.put(image)
        logger.info(f"管理器 {self.manager_id} 接收图片数据，当前队列为：{self.image_queue.qsize()}")
    
    def add_offline_data(self, data: Any):
        """离线模式下，添加图片、视频和参数到队列"""
        if not self.is_active:
            error_msg = f"管理器 {self.manager_id} 未启动"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        self.offline_data_queue.put(data)
        logger.info(f"管理器 {self.manager_id} 接收数据，当前队列为：{self.offline_data_queue.qsize()}")
    
    def stop_session(self):
        """停止模型会话"""
        logger.info(f"正在停止管理器 {self.manager_id} 会话")
        
        with self._lock:
            if not self.is_active:
                logger.warning(f"管理器 {self.manager_id} 已非活跃状态")
                return
            
            # 设置停止标志
            self._stop_event.set()
            
            # 调用模型的stop函数
            try:
                if self.type == "chat":
                    self.add_offline_data({
                        "stop_offline_generate": True
                    })
                    logger.info(f"模型 {self.manager_id} 传递stop_offline_generate信号")
                else:
                    self.model_instance.stop_real_time_generate()
                    logger.info(f"调用模型 {self.manager_id} stop函数成功")
            except Exception as e:
                logger.error(f"调用模型 {self.manager_id} stop函数失败: {e}")
                raise RuntimeError(f"调用模型 {self.manager_id} stop函数失败: {e}")
        
        # 等待real_time_generate完成
        logger.info(f"等待管理器 {self.manager_id} 的generate完成...")
        timeout_count = 0
        max_timeout = 30  # 最多等待30秒
        
        while not self.is_stop and timeout_count < max_timeout:
            time.sleep(0.1)  # 每100ms检查一次
            timeout_count += 0.1
        
        if not self.is_stop:
            logger.warning(f"管理器 {self.manager_id} 等待real_time_generate完成超时，强制继续")
            raise RuntimeError(f"管理器 {self.manager_id} 等待real_time_generate完成超时，强制继续")
        else:
            logger.info(f"管理器 {self.manager_id} 的real_time_generate已完成")
            
        with self._lock:
            current_thread = threading.current_thread()
            # 等待线程结束
            if self._generate_thread and self._generate_thread.is_alive():
                if self._generate_thread != current_thread:
                    self._generate_thread.join(timeout=2.0)
                    if self._generate_thread.is_alive():
                        logger.warning(f"管理器 {self.manager_id} 生成线程未能正常结束")
                else:
                    logger.info(f"管理器 {self.manager_id} 跳过join生成线程（当前线程）")
            
            if self._monitor_thread and self._monitor_thread.is_alive():
                if self._monitor_thread != current_thread:
                    self._monitor_thread.join(timeout=2.0)
                    if self._monitor_thread.is_alive():
                        logger.warning(f"管理器 {self.manager_id} 监听线程未能正常结束")
                else:
                    logger.info(f"管理器 {self.manager_id} 跳过join监听线程（当前线程）")
                        
            
            # 清空所有队列
            self._clear_queue(self.prompt_queue)
            self._clear_queue(self.image_queue)
            self._clear_queue(self.token_queue)
            
            # 重置状态
            self.is_active = False
            self._token_callback = None
            
        logger.info(f"管理器 {self.manager_id} 会话已停止")
    
    def _clear_queue(self, queue: Queue):
        """清空队列"""
        while True:
            try:
                queue.get_nowait()
            except Empty:
                break
    
    def get_status(self) -> dict:
        """获取管理器状态"""
        return {
            'manager_id': self.manager_id,
            'is_active': self.is_active,
            'created_at': self.created_at,
            'last_used': self.last_used,
            'queue_sizes': {
                'prompt_queue': self.prompt_queue.qsize(),
                'image_queue': self.image_queue.qsize(),
                'token_queue': self.token_queue.qsize(),
                'offline_data_queue': self.offline_data_queue.qsize()
            },
            'threads_alive': {
                'generate': self._generate_thread.is_alive() if self._generate_thread else False,
                'monitor': self._monitor_thread.is_alive() if self._monitor_thread else False
            }
        }


class ModelPool:
    """模型池，只负责模型的分发和回收"""
    
    def __init__(self, pool_size: int = 1, model_class=None):
        self.pool_size = pool_size
        self.model_class = model_class
        self._available_managers = Queue(maxsize=pool_size)
        self._all_managers: List[ModelManager] = []
        self._active_managers = {}  # {manager_id: ModelManager}
        self._lock = threading.Lock()
        
        # 初始化模型池
        self._initialize_pool()
        
        logger.info(f"模型池初始化完成，总共 {pool_size} 个模型管理器")
    
    def _initialize_pool(self):
        """初始化模型池"""
        for i in range(self.pool_size):
            manager_id = f"manager_{i+1}_{uuid.uuid4().hex[:8]}"
            gpu_id = i
            frame_extract_num_threads = 1
            
            manager = ModelManager(manager_id, frame_extract_num_threads, gpu_id)
            self._all_managers.append(manager)
            self._available_managers.put(manager)
    
    def acquire_model(self, timeout: float = 0.1) -> Optional[ModelManager]:
        """
        获取一个可用的模型管理器
        
        Args:
            timeout: 获取超时时间（秒）
            
        Returns:
            ModelManager 或 None
        """
        try:
            manager = self._available_managers.get(timeout=timeout)
            
            with self._lock:
                self._active_managers[manager.manager_id] = manager
            
            logger.info(f"分配模型管理器: {manager.manager_id}")
            return manager
            
        except Empty:
            logger.warning("没有可用的模型管理器")
            return None
    
    def release_model(self, manager: ModelManager):
        """
        回收模型管理器
        
        Args:
            manager: 要回收的模型管理器
        """
        if manager not in self._all_managers:
            logger.error(f"尝试回收不属于此池的管理器: {manager.manager_id}")
            return
        
        try:
            # 停止会话
            manager.stop_session()
            
            with self._lock:
                # 从活跃列表中移除
                if manager.manager_id in self._active_managers:
                    del self._active_managers[manager.manager_id]
                
                # 放回可用池
                self._available_managers.put_nowait(manager)
            
            logger.info(f"模型管理器已回收: {manager.manager_id}")
            
        except Exception as e:
            logger.error(f"回收模型管理器失败 {manager.manager_id}: {e}")
    
    def available_count(self) -> int:
        """获取可用模型数量"""
        return self._available_managers.qsize()
    
    def active_count(self) -> int:
        """获取活跃模型数量"""
        with self._lock:
            return len(self._active_managers)
    
    def get_pool_status(self) -> dict:
        """获取模型池状态"""
        with self._lock:
            active_managers = list(self._active_managers.values())
        
        return {
            'total_managers': len(self._all_managers),
            'available_managers': self.available_count(),
            'active_managers': self.active_count(),
            'managers_status': [manager.get_status() for manager in self._all_managers],
            'active_sessions': [manager.get_status() for manager in active_managers]
        }
    
    def force_release_all(self):
        """强制释放所有模型管理器"""
        logger.warning("强制释放所有模型管理器")
        
        with self._lock:
            # 停止所有活跃会话
            for manager in list(self._active_managers.values()):
                try:
                    manager.stop_session()
                    logger.info(f"强制停止管理器: {manager.manager_id}")
                except Exception as e:
                    logger.error(f"强制停止管理器 {manager.manager_id} 失败: {e}")
            
            # 清空活跃列表
            self._active_managers.clear()
            
            # 重新填充可用队列
            while not self._available_managers.empty():
                try:
                    self._available_managers.get_nowait()
                except Empty:
                    break
            
            for manager in self._all_managers:
                self._available_managers.put(manager)
    
    def shutdown(self):
        """关闭模型池"""
        logger.info("正在关闭模型池...")
        
        # 强制释放所有管理器
        self.force_release_all()
        
        logger.info("模型池已关闭")
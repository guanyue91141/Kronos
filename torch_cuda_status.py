#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PyTorch和CUDA状态检测脚本
该脚本检测并显示当前PyTorch和CUDA的状态
"""

import sys

def check_torch_cuda_status():
    """检查并显示PyTorch和CUDA状态信息"""
    print("=" * 60)
    print("PyTorch和CUDA状态检测")
    print("=" * 60)
    
    try:
        import torch
        print(f"PyTorch版本: {torch.__version__}")
    except ImportError:
        print("PyTorch未安装")
        return

    # 检查CUDA可用性
    cuda_available = torch.cuda.is_available()
    print(f"CUDA可用: {cuda_available}")

    # 检查是否构建时包含CUDA
    built_with_cuda = torch.version.cuda is not None
    print(f"编译时包含CUDA: {built_with_cuda}")

    if cuda_available:
        print(f"CUDA版本: {torch.version.cuda}")
        if hasattr(torch.backends, 'cudnn') and torch.backends.cudnn.version():
            print(f"cuDNN版本: {torch.backends.cudnn.version()}")
            print(f"cuDNN可用: {torch.backends.cudnn.enabled}")
        
        print(f"CUDA设备数量: {torch.cuda.device_count()}")
        
        for i in range(torch.cuda.device_count()):
            print(f"  设备 {i}: {torch.cuda.get_device_name(i)}")
            print(f"    计算能力: {torch.cuda.get_device_capability(i)}")
            
            # 检查内存信息
            total_memory = torch.cuda.get_device_properties(i).total_memory
            print(f"    总内存: {total_memory / 1024**3:.2f} GB")
            
            # 检查当前内存使用情况
            allocated_memory = torch.cuda.memory_allocated(i)
            cached_memory = torch.cuda.memory_reserved(i)
            print(f"    已分配内存: {allocated_memory / 1024**2:.2f} MB")
            print(f"    缓存内存: {cached_memory / 1024**2:.2f} MB")
        
        # 检查当前设备
        current_device = torch.cuda.current_device()
        print(f"当前CUDA设备ID: {current_device}")
    else:
        print("CUDA不可用，PyTorch将仅使用CPU")
    
    # 检查MPS (Apple Silicon) 如果CUDA不可用
    if not cuda_available and hasattr(torch.backends, 'mps'):
        mps_available = torch.backends.mps.is_available()
        print(f"MPS (Apple Silicon) 可用: {mps_available}")
    
    # 显示当前设备
    if cuda_available:
        current_device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        print(f"当前设备: {current_device}")
        if cuda_available:
            print(f"当前设备名称: {torch.cuda.get_device_name() if torch.cuda.device_count() > 0 else 'N/A'}")
    elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        print("当前设备: mps (Apple Silicon)")
    else:
        print("当前设备: cpu")

    # 检查BLAS库信息
    if hasattr(torch, 'has_mkl') and torch.has_mkl:
        print(f"Intel MKL库可用: {torch.has_mkl}")
    if hasattr(torch, 'has_magma'):
        print(f"MAGMA库可用: {torch.has_magma}")
    
    # 检查其他重要特性
    print(f"OpenMP可用: {torch.backends.openmp.is_available()}")
    if hasattr(torch.backends, 'mkldnn') and hasattr(torch.backends.mkldnn, 'is_available'):
        print(f"MKLDNN可用: {torch.backends.mkldnn.is_available()}")
    
    # 创建一个张量测试GPU功能
    print("\n--- 功能测试 ---")
    try:
        # 简单张量操作测试
        test_tensor = torch.tensor([1.0, 2.0, 3.0])
        print(f"CPU张量测试: {test_tensor}")
        
        if cuda_available and torch.cuda.device_count() > 0:
            # GPU张量操作测试
            gpu_tensor = test_tensor.cuda()
            print(f"GPU张量测试: {gpu_tensor}")
            print("GPU功能正常")
        else:
            print("无法进行GPU功能测试")
    except Exception as e:
        print(f"功能测试出错: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    check_torch_cuda_status()
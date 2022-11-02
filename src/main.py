#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/31 6:06 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : main.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
# 导入numpy验证docker是否根据requirements文件安装好了依赖
import numpy as np
import os
if __name__ == '__main__':
    # 在logs目录下创建个名为docker.log的空文件，验证容器和宿主机是否能进行数据同步
    os.mknod('../logs/docker.log')
    print("hello docker")

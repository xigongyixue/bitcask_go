#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os, sys

# 打开文件
fd = os.open( "foo.txt", os.O_RDWR|os.O_CREAT )

# 写入字符串
os.write(fd, b"GeeksforGeeks")

# 使用 fsync() 方法.
os.fsync(fd)

# 读取内容
os.lseek(fd, 0, 0)
str = os.read(fd, 100)
print(str)

# 关闭文件
os.close( fd)


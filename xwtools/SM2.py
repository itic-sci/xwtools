#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: SM2.py
Date: 2021/5/28 1:50 下午
'''

"""
1- 国产非对称加密算法，后端生成公钥给前端，前端通过公钥加密后反给后端，后端通过私钥解密；
2- 不可逆加密密码和验证密码是否正确
"""

from gmssl import sm2, func


# (pysmx) pip install snowland-smx==0.3.2a1
def generate_keys():
    from pysmx.SM2 import generate_keypair
    pk, sk = generate_keypair()
    print(f'public_key: {pk.hex()}')
    print(f'private_key: {sk.hex()}')
    return pk.hex(), sk.hex()


class EncryptBySM2(object):
    def __init__(self, public_key, private_key):
        self.__sm2_crypt = sm2.CryptSM2(public_key=public_key, private_key=private_key)

    def encrypt(self, content: str):
        # 16进制的公钥和私钥
        enc_data = self.__sm2_crypt.encrypt(content.encode()).hex()
        return enc_data

    def decrypt(self, enc_data: str):
        dec_data = self.__sm2_crypt.decrypt(bytes.fromhex(enc_data)).decode()
        return dec_data

    def encrypt_password(self, password: str):
        salt = func.random_hex(self.__sm2_crypt.para_len)
        enc_password = self.__sm2_crypt.sign(password.encode(), salt)  # 16进制
        return enc_password

    def ver_password(self, password: str, enc_password: str):
        ver = self.__sm2_crypt.verify(enc_password, password.encode())  # 16进制
        return ver


if __name__ == '__main__':
    private_key = 'ca28c0f5bb4588df803b6ccbd030026f0d395384bc4d68783099044234cafaac'
    public_key = 'bdf27a47becf268fcde4336d965f23fc8c14039d1fe89144d98de23f01873899442e5ef956fe1a5a7c3fe32e5c84ebe9867f6a92f48255a0d86ce4b72252a624'

    # 生成公钥， 私钥
    # public_key,private_key = generate_keys()

    encSM2 = EncryptBySM2(public_key=public_key, private_key=private_key)

    # content = "这是一个测试"
    # enc_data = encSM2.encrypt(content)
    # print(enc_data)
    # dec_data = encSM2.decrypt(enc_data)
    # print(dec_data)

    password = "xuwei2020"
    enc_password = encSM2.encrypt_password(password)
    print(enc_password)
    ver = encSM2.ver_password(password, enc_password)
    print(ver)

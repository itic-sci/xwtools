import base64
import hashlib

'''
采用AES对称加密算法
pip3 install -i https://pypi.douban.com/simple pycryptodome
'''

keyStr = "sdf8g4edxuweif80"


class Encryption(object):

    @classmethod
    def encrypt_key(cls, text):
        from Crypto.Cipher import AES
        private_key = hashlib.sha256(keyStr.encode()).digest()
        rem = len(text) % 16
        padded = str.encode(text) + (b'\0' * (16 - rem)) if rem > 0 else str.encode(text)
        iv = bytes([0] * 16)
        cipher = AES.new(private_key, AES.MODE_CFB, iv, segment_size=128)
        enc = cipher.encrypt(padded)[:len(text)]
        return base64.b64encode(iv + enc).decode()

    @classmethod
    def decrypt_key(cls, text):
        from Crypto.Cipher import AES
        private_key = hashlib.sha256(keyStr.encode()).digest()
        text = base64.b64decode(text)
        iv, value = text[:16], text[16:]
        rem = len(value) % 16
        padded = value + (b'\0' * (16 - rem)) if rem > 0 else value
        cipher = AES.new(private_key, AES.MODE_CFB, iv, segment_size=128)
        return (cipher.decrypt(padded)[:len(value)]).decode()

    @classmethod
    def encryt_dict(cls, config_map, ignore=None):
        # ignore 为忽略的字段，默认值是none，值为list，如['a', 'b']
        _res = {}
        for _k, _v in config_map.items():
            if ignore and _k in ignore:
                _res[_k] = _v
                continue
            _res[_k] = cls.encrypt_key(_v)
        return _res

    @classmethod
    def decrypt_dict(cls, config_map, ignore=None):
        # ignore 为忽略的字段，默认值是none，值为list，如['a', 'b']
        _res = {}
        for _k, _v in config_map.items():
            if ignore and _k in ignore:
                _res[_k] = _v
                continue
            try:
                _res[_k] = cls.decrypt_key(_v)
            except:
                _res[_k] = _v
        return _res


import bcrypt


def encrypt_by_bcrypt(password: str):
    # 用一个随机的盐值来加密密码
    # bcrypt.gensalt() 还可以接受一个参数来控制它要计算多少次，默认是 12
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
    # 下面这个 hashed 就是加密后的值
    return hashed.decode()


def verify_password_by_bcrypt(password: str, encrypt_password: str):
    # 验证明文密码是不是加密后的值
    return bcrypt.checkpw(password.encode(), encrypt_password.encode())


if __name__ == '__main__':
    r = Encryption.encrypt_key("150204200112110016")
    print(r == "AAAAAAAAAAAAAAAAAAAAADvOo1yAl/RC4cAvHim6QOQqCA==")
    print(Encryption.decrypt_key(r))

    password = '150204200112110016'
    encrypt_password = encrypt_by_bcrypt(password)
    print(encrypt_password)
    print(verify_password_by_bcrypt(password, encrypt_password))

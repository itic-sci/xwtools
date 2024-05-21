# 引入哈希库
import hashlib
import uuid


def get_md5(s):
    s = s.encode('utf8')
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()


def get_sha224(s):
    code = hashlib.sha3_256(s.encode('utf8'))
    return code.hexdigest()


code_map = (
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
    'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
    'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z', '0', '1', '2', '3', '4', '5',
    '6', '7', '8', '9', 'A', 'B', 'C', 'D',
    'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
    'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
    'U', 'V', 'W', 'X', 'Y', 'Z'
)


def get_hash_key(long_url):
    hkeys = []
    hex = get_sha224(long_url)
    for i in range(0, 8):
        n = int(hex[i * 8:(i + 1) * 8], 16)
        v = []
        e = 0
        for j in range(0, 7):
            x = 0x0000003D & n
            e |= ((0x00000002 & n) >> 1) << j
            v.insert(0, code_map[x])
            n = n >> 6
        e |= n << 5
        v.insert(0, code_map[e & 0x0000003D])
        hkeys.append(''.join(v))
    return hkeys


def get_url_uuid_list(long_url):
    uuid_list = get_hash_key(long_url)
    return uuid_list


namespace = uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8')


def get_uuid5(short_text):
    return str(uuid.uuid5(namespace, short_text))


if __name__ == '__main__':
    r = get_uuid5('xxx')
    print(type(r))

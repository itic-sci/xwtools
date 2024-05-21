from datetime import datetime, timedelta


class JwtToken(object):
    _expire_message = dict(code=1200, msg="token 已经失效")
    _unknown_error_message = dict(code=4200, msg="token 解析失败")

    @classmethod
    def generate_token(cls, payload: dict, expSeconds: int, salt="123xw123") -> str:
        """
        :param payload: 信息体
        :param expSeconds: 过期时间
        :return:
        """
        import jwt

        payload["exp"] = datetime.utcnow() + timedelta(seconds=expSeconds)
        headers = dict(typ="jwt", alg="HS256")
        resut = jwt.encode(payload=payload, key=salt, algorithm="HS256", headers=headers)
        return resut

    @classmethod
    def parse_token(cls, token: str, salt="123xw123") -> tuple:
        import jwt

        verify_status = False
        try:
            payload_data = jwt.decode(token, salt, algorithms=['HS256'])
            verify_status = True
        except jwt.ExpiredSignatureError:
            payload_data = cls._expire_message
        except Exception as _err:
            payload_data = cls._unknown_error_message
        return verify_status, payload_data


if __name__ == '__main__':
    TEST_DATA = dict(name="mooor")
    token = JwtToken.generate_token(TEST_DATA, 10, salt="123xw1221")
    print(token)
    payload = JwtToken.parse_token(token, salt="123xw1221")
    print(payload)

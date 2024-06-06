import jwt
from flask_httpauth import HTTPTokenAuth

from config.settings import JWT_SECRET, PASSWORD, USER

auth = HTTPTokenAuth('Bearer')


@auth.verify_token
def verify_token(token):
    try:
        jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        return True
    except Exception as e:
        return False
    # if all([data.get("password") == PASSWORD, data.get("username") == USER]):
    #     return True
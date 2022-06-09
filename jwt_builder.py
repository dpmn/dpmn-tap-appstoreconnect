import jwt
import os

from datetime import datetime, timedelta, timezone


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class JWTBuilder:
    def __init__(self, key_id, key_file, issuer_id):
        self._key_id = key_id
        self._issuer_id = issuer_id
        self._key_path = os.path.join(BASE_DIR, 'secrets', key_file)

    def build(self):
        with open(self._key_path, 'r') as fh:
            signing_key = fh.read()

        current_time = datetime.now(timezone.utc) - timedelta(seconds=30)
        expiration_time = int((current_time + timedelta(minutes=20)).timestamp())

        payload = {
            'iss': self._issuer_id,
            'iat': int(current_time.timestamp()),
            'exp': expiration_time,
            'aud': 'appstoreconnect-v1'
        }

        encoded_jwt = jwt.encode(
            payload=payload,
            key=signing_key,
            algorithm='ES256',
            headers={
                'kid': self._key_id
            }
        )

        return encoded_jwt

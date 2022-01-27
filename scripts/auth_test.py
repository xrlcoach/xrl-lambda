import boto3
import hmac
import hashlib
import rsa
import base64
import json
import urllib3
from datetime import datetime
import jwt

USER_POOL_ID = 'ap-southeast-2_X405VGEIl'
CLIENT_ID = '53irugvhakp6kd5cmd2o75kn'
client = None

def initiate_auth(username, password):
    try:
        resp = client.admin_initiate_auth(
            UserPoolId=USER_POOL_ID,
            ClientId=CLIENT_ID,
            AuthFlow='ADMIN_NO_SRP_AUTH',
            AuthParameters={
                'USERNAME': username,
                'PASSWORD': password
            },
            ClientMetadata={
                'username': username,
                'password': password
            })
    except client.exceptions.NotAuthorizedException as e:
        return None, "The username or password is incorrect"
    except client.exceptions.UserNotFoundException as e:
        return None, "The username or password is incorrect"
    except client.exceptions.UserNotConfirmedException as e:
        return None, "This account has not been confirmed yet. Contact XRL Admin to obtain authorisation."
    except Exception as e:
        print(e)
        return None, "Unknown error"
    return resp, None
    
    
def refresh_auth(username, refresh_token):
    try:
        resp = client.admin_initiate_auth(
            UserPoolId=USER_POOL_ID,
            ClientId=CLIENT_ID,
            AuthFlow='REFRESH_TOKEN_AUTH',
            AuthParameters={
                'REFRESH_TOKEN': refresh_token,
                #'SECRET_HASH': get_secret_hash(username)
            },
            ClientMetadata={            })
    except client.exceptions.NotAuthorizedException as e:
        return None, "The username or password is incorrect"
    except client.exceptions.UserNotFoundException as e:
        return None, "The username or password is incorrect"
    except Exception as e:
        print(e)
        return None, "Unknown error"
    return resp, None
    
def lambda_handler():
    global client
    if client == None:
        client = boto3.client('cognito-idp')

    username = 'admin'
    password = 'pA$5word'    
    
    resp = initiate_auth(username, password)
    if resp[0] != None:
        id_token = resp[0]['AuthenticationResult']['IdToken']
        header_encoded = id_token.split('.')[0]
        payload_encoded = id_token.split('.')[1]
        #head_payload_encoded = header_encoded + '.' + payload_encoded
        sig = id_token.split('.')[2]
        #sig_decoded = base64.b64decode(sig)

        header = json.loads(base64.b64decode(header_encoded + '========='))
        print("Header: " + str(header))
        payload = json.loads(base64.b64decode(payload_encoded + '========='))
        print("Payload: " + str(payload))
        # sig = json.loads(base64.b64decode(sig_encoded + '========='))
        print("Signature: " + str(sig))

        if payload['aud'] == '53irugvhakp6kd5cmd2o75kn':
            print("Audience matches")
        else:
            print("Audience DOES NOT match")
        if payload['iss'] == 'https://cognito-idp.ap-southeast-2.amazonaws.com/ap-southeast-2_X405VGEIl':
            print("Client matches")
        else:
            print("Client DOES NOT match")
        print(str(payload['exp']))
        print(datetime.now().timestamp())
        if int(payload['exp']) > int(datetime.now().timestamp()):
            print("Token is not expired")
        else:
            print("Token has EXPIRED")


        # http = urllib3.PoolManager()
        # jwkResp = http.request('GET', 'https://cognito-idp.ap-southeast-2.amazonaws.com/ap-southeast-2_X405VGEIl/.well-known/jwks.json')
        # print(jwkResp.status)
        # key_list = json.loads(jwkResp.data)
        # print(key_list)
        # key_index = -1
        # public_key = ''
        # for index, key in enumerate(key_list['keys']):
        #     if header['kid'] == key['kid']:
        #         print("KID found")
        #         key_index = index
        #         public_key = jwk.jwk.JWK(**key)
        #         print(public_key.export_to_pem())
        #         print(jwt.verify_jwt)
        # if key_index == -1:
        #     print("KID NOT found")

        jwks_client = jwt.PyJWKClient('https://cognito-idp.ap-southeast-2.amazonaws.com/ap-southeast-2_X405VGEIl/.well-known/jwks.json')
        signing_key = jwks_client.get_signing_key_from_jwt(id_token)

        data = jwt.decode(id_token, signing_key.key, algorithms=['RS256'], audience=payload['aud'])
        print("JWT verified")
        print(data)
       

        # head_payload_encoded_hash = hashlib.sha256(head_payload_encoded.encode('utf-8'))
        
        # current_hash_encrypt = rsa.encrypt(head_payload_encoded_hash, rsa.verify())
        # current_hash_encrypt_encoded = base64.b64encode(current_hash_encrypt)
        # current_hash_value = hashlib.sha256(current_hash_encrypt_encoded)
        # original_hash_value = rsa.decrypt(sig, json.dumps(public_key))
        



    
    else:
        print("Error: " + str(resp[1]))  
    
if __name__ == '__main__':
    lambda_handler()
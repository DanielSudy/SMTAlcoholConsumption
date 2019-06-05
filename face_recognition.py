import requests
import json
import pandas as pd



subscription_key = "1e83a4fcc02c48bbb44fd69dc4d17cdd"
assert subscription_key

face_api_url = 'https://westcentralus.api.cognitive.microsoft.com/face/v1.0/detect'

#image_url = 'https://upload.wikimedia.org/wikipedia/commons/3/37/Dagestani_man_and_woman.jpg'
#image_url = 'https://www.oefb.at/oefb2/images/1278650591628556536_835d47617ed7641d656a-1,0-640x480-640x480.png'
headers = {'Ocp-Apim-Subscription-Key': subscription_key}

params = {
    'returnFaceId': 'true',
    'returnFaceLandmarks': 'false',
    'returnFaceAttributes': 'age,gender,headPose,smile,facialHair,glasses,emotion,hair,makeup,occlusion,accessories,blur,exposure,noise',
}

class MSAzureFaceRecogntion():
    def getFaceInfos(self,url):

        df = pd.DataFrame()
        faceID=[]
        gender=[]
        age=[]
        response = requests.post(face_api_url, params=params, headers=headers, json={"url": url})
        result=json.dumps(response.json())

        data = json.loads(result)
        if(len(data)==0):
            #print("No recognition")
            return False,None
        else:
            for i in range(0, len(data)):
                faceID.append(data[i]["faceId"])
                gender.append(data[i]["faceAttributes"]["gender"])
                age.append(data[i]["faceAttributes"]["age"])
                #print(data[i]["faceId"])
                #print(data[i]["faceAttributes"]["gender"])
                #print(data[i]["faceAttributes"]["age"])
            df['face_id'] = faceID
            df['gender'] = gender
            df['age'] = age
            return True,df
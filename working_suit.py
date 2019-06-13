import re

class WorkingSuit():
    def normalizeTwitterImageName(self,url):
        return url.replace('_normal','')

    def getMeanGender(self,NGender,FGender):
        if(NGender=="male" and FGender=="male"):
            return "male"
        if (NGender == "female" and FGender == "female"):
            return "female"
        if (NGender == "mostly_female" and FGender == "female"):
            return "female"
        if (NGender == "mostly_male" and FGender == "male"):
            return "male"
        if (NGender == "mostly_male" and FGender == "female"):
            return "female"
        if (NGender == "mostly_female" and FGender == "male"):
            return "male"
        if (NGender == "unknown" and FGender == "female"):
            return "female"
        if (NGender == "unknown" and FGender == "male"):
            return "male"
        if (NGender == "andy" and FGender == "male"):
            return "male"
        if (NGender == "andy" and FGender == "female"):
            return "female"
        if (NGender == "female" and FGender == "male"):
            return "male"
        if (NGender == "male" and FGender == "female"):
            return "female"


    def getNormalizedGenderFromName(self,Gender):
        if(Gender=="mostly_female"):
            return "female"
        elif(Gender=="mostly_male"):
            return "male"
        else:
            return Gender


import countries as country



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

    def checkIfCountryIsInEurope(self,countryCode):
        for items in country.countries:
            if(countryCode==items['code']):
               if(items['timezones'][0].split("/")[0]=="Europe"):
                   return True,items['name']
               else:
                   return False,items['name']

        return False,"NF"

    def getContinetalInfo(self,countryCode):
        for items in country.countries:
            if(countryCode == items['code']):
                return True,items['timezones'][0].split("/")[0]
        return False,None

    def getAgeClass(self, age):
        if(age<=2):
            return "Baby"
        elif((age>2) and (age<14)):
            return "Child"
        elif((age>=14) and (age<20)):
            return "Teenager"
        elif ((age >= 20) and (age < 35)):
            return "Young Adults"
        elif ((age >= 35) and (age < 50)):
            return "Adults"
        elif ((age >= 50) and (age < 65)):
            return "Older Adults"
        elif ((age >= 65)):
            return "Seniors"
        else:
            return "Undefined"






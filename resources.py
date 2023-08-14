from dataclasses import dataclass
import json
import datetime 
from textwrap import dedent
import pandas as pd
import pytz
import typing
from typing import List


def pull_item_from_extension(url, extensions) -> List:

    matches = list(
        filter(
        lambda x: x['url'] == url, extensions)
    )
    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0]
    else:
        return matches

def get_nested_attr(obj, attr, level_sep='_'):
    # print(obj, attr)
    if attr.__contains__(level_sep):
        # print('recursion')
        attrs = attr.split(level_sep)
        next_level = getattr(obj, attrs[0])
        if isinstance(next_level, list):
            return get_nested_attr(getattr(obj, attrs[0])[0], attrs[1])
        else:    
            return get_nested_attr(getattr(obj, attrs[0]), attrs[1])
    else:
        return getattr(obj, attr)

def to_string(item):
    if item is None:
        return 'None'
    elif isinstance(item, datetime.datetime):
        return item.isoformat()
    elif isinstance(item, datetime.date):
        return item.isoformat()
    elif isinstance(item, str):
        return item
    elif isinstance(item, int):
        return str(item)
    elif isinstance(item, float):
        return f"{item:.4f}"
    elif isinstance(item, list):
        return ','.join(item)
    else:
        print(f'error with {item}')


def make_csv_header(obj, filepath):
    with open(filepath, 'w') as file:
        file.write(', '.join(obj.columns))
        file.write('\n')


@dataclass
class Concept:
    system: str
    code: str
    text: str

@dataclass
class Practitioner:
    npi: str
    name: str

@dataclass
class Provider:
    id: str
    name: str


@dataclass
class Address:
    line: list
    city: str
    state: str
    postalCode: str
    country: str

@dataclass
class Name:
    use: str
    given: list
    family: list
    # prefix: list


class EHR_item:
    
    columns = []
    
    def __init__(self):
        pass

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        row = {}
        for attr in self.columns:
            item = get_nested_attr(self, attr)
            row[attr] =to_string(item)

        df = pd.DataFrame([row])
        return df

    def to_csv(self, filepath=None, header=False, sep=',') -> str:
        df = self.to_dataframe()
        if filepath:
            return df.to_csv(path_or_buf=filepath, index=False, header=header, sep=sep)
        else:
            return df.to_csv(index=False, header=header, sep=sep)

    def to_sql_insert(self, table_name):
        
        df = self.to_dataframe()
        
        column_names = ', '.join([str(x) for x in df.columns])
        values = ', '.join([f'"{str(x)}"' for x in df.iloc[0].values])
        return f"INSERT INTO {table_name} ({column_names}) VALUES ({values});"

    


class Encounter(EHR_item):

    columns = [
            'id',
            'startTime',
            'endTime',
            'status',
            'patientId',
            'encounterType_system',
            'encounterType_code',
            'encounterType_text',
            'practitioner_npi',
            'practitioner_name',
            'provider_id',
            'provider_name'
        ]

    def __init__(self, data, today=None):
        self.id = data['id']
        self.status = data['status']
        
        tmp = data['type'][0]['coding'][0]
        self.encounterType = Concept(system=tmp['system'], code=tmp['code'], text=tmp['display'])

        self.patientId = data['patient']['reference'].split(':')[-1]
        
        tmp = data['participant'][0]['individual']
        self.practitioner = Practitioner(
            npi=tmp['reference'].split('|')[-1],
            name=tmp['display']
        )

        self.startTime = datetime.datetime.fromisoformat(data['period']['start'])
        self.endTime = datetime.datetime.fromisoformat(data['period']['end'])

        self.provider = Provider(
            id=data['serviceProvider']['reference'].split('|')[-1],
            name=data['serviceProvider']['display']
        )

    def __repr__(self):
        return "Encounter(\n  "+"\n  ".join([f"{key} = {value}" for (key,value) in self.__dict__.items()])+"\n)"


class MedicationOrder(EHR_item):

    columns = [
            "id", 
            "dateWritten", 
            "status", 
            "patientId", 
            "encounterId", 
            "prescriber_npi", 
            "prescriber_name", 
            "medication_system", 
            "medication_code", 
            "medication_text"
        ]

    def __init__(self, data, today=None):
        self.id = data['id']
        self.dateWritten = datetime.datetime.fromisoformat(data['dateWritten'])
        self.status = data['status']
        self.patientId = data['patient']['reference'].split(':')[-1]
        self.encounterId = data['encounter']['reference'].split(':')[-1]
        self.prescriber = Practitioner(
            npi=data['prescriber']['reference'].split('|')[-1],
            name=data['prescriber']['display']
        )
        tmp = data['medicationCodeableConcept']['coding'][0]
        self.medication = Concept(system=tmp['system'], code=tmp['code'], text=tmp['display'])

    def __repr__(self):
        return "MedicationOrder(\n  "+"\n  ".join([f"{key} = {value}" for (key,value) in self.__dict__.items()])+"\n)"


class Condition(EHR_item):

    columns = [
            'id',
            'patientId',
            'encounterId',
            'dateRecorded',
            'code_system',
            'code_code',
            'code_text',
            'category_system',
            'category_code',
            'category_text',
            'clinicalStatus',
            'verificationStatus',
            'onsetDateTime',
            'abatementDateTime'
        ]

    def __init__(self, data, today=None):
        self.id = data['id']
        self.patientId = data['patient']['reference'].split(':')[-1]
        self.encounterId = data['encounter']['reference'].split(':')[-1]
        self.dateRecorded = datetime.datetime.fromisoformat(data['dateRecorded']).astimezone(pytz.timezone('US/Eastern'))
        tmp = data['code']['coding'][0]
        self.code = Concept(system=tmp['system'], code=tmp['code'], text=tmp['display'])
        
        tmp = data['category']['coding'][0]
        self.category = Concept(system=tmp['system'], code=tmp['code'], text=tmp['code'])

        self.clinicalStatus = data['clinicalStatus']
        self.verificationStatus = data['verificationStatus']
        self.onsetDateTime = datetime.datetime.fromisoformat(data['onsetDateTime'])
        if 'abatementDateTime' in data.keys(): 
            self.abatementDateTime = datetime.datetime.fromisoformat(data['abatementDateTime'])
        else:
            self.abatementDateTime = None

    def __repr__(self):
        return "Condition(\n  "+"\n  ".join([f"{key} = {value}" for (key,value) in self.__dict__.items()])+"\n)"


class Patient(EHR_item):

    patient_data_urls = {
        "race":"http://hl7.org/fhir/StructureDefinition/us-core-race",
        "ethnicity":"http://hl7.org/fhir/StructureDefinition/us-core-ethnicity",
        "mothersMaidenName": "http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName", 
        "birthSex": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex", 
        "birthPlace": "http://hl7.org/fhir/StructureDefinition/birthPlace", 
        "disabilityAdjustedLifeYears": "http://synthetichealth.github.io/synthea/disability-adjusted-life-years", 
        "qualityAdjustedLifeYears": "http://synthetichealth.github.io/synthea/quality-adjusted-life-years"
            
        }

    columns = [
            'id',
            'gender',
            'birthDate',
            'deceasedDateTime',
            # 'address_line',
            # 'address_city',
            # 'address_state',
            # 'address_postalCode',
            # 'address_country',
            # 'name_prefix',
            'name_given',
            'name_family',
            'name_use',
            'maritalStatus',
            'race',
            'ethnicity',
            'mothersMaidenName',
            'birthSex',
            'disabilityAdjustedLifeYears',
            'qualityAdjustedLifeYears',
            'birthPlace_city',
            'birthPlace_state',
            'birthPlace_country',
            'isDeceased'
        ]

    def __init__(self, data, today=None):
        self.id = data['id']
        self.gender = data['gender']
        self.birthDate = datetime.datetime.fromisoformat(data['birthDate']).astimezone(pytz.timezone('US/Eastern'))
       
        # self.address = [
        #         Address(
        #             line = data['address'][n]['line'],
        #             city = data['address'][n]['city'],
        #             state = data['address'][n]['state'],
        #             postalCode = data['address'][n]['postalCode'],
        #             country = data['address'][n]['country'],
        #         ) for n in range(len(data['address']))
        #         ]
        self.name = [
            Name(
                use=data['name'][n]['use'],
                given=data['name'][n]['given'],
                family=data['name'][n]['family'],
                # prefix=data['name'][n]['prefix'],
            ) for n in range(len(data['name']))
            ]
        self.maritalStatus = data['maritalStatus']['coding'][-1]['code']
        
        tmp = pull_item_from_extension(self.patient_data_urls['race'], data['extension'])
        self.race = tmp['valueCodeableConcept']['coding'][0]['display']

        tmp = pull_item_from_extension(self.patient_data_urls['ethnicity'], data['extension'])
        self.ethnicity = tmp['valueCodeableConcept']['coding'][0]['display']

        tmp = pull_item_from_extension(self.patient_data_urls['mothersMaidenName'], data['extension'])
        self.mothersMaidenName = tmp['valueString']

        tmp = pull_item_from_extension(self.patient_data_urls['birthSex'], data['extension'])
        self.birthSex = tmp['valueCode']

        tmp = pull_item_from_extension(self.patient_data_urls['disabilityAdjustedLifeYears'], data['extension'])
        self.disabilityAdjustedLifeYears = tmp['valueDecimal']

        tmp = pull_item_from_extension(self.patient_data_urls['qualityAdjustedLifeYears'], data['extension'])
        self.qualityAdjustedLifeYears = tmp['valueDecimal']

        tmp = pull_item_from_extension(self.patient_data_urls['birthPlace'], data['extension'])
        self.birthPlace = Address(
            line=[],
            city=tmp['valueAddress']['city'],
            state=tmp['valueAddress']['state'], 
            country=tmp['valueAddress']['country'], 
            postalCode=''
        )

        if today is None:
            today = datetime.date.today()

            
        if 'deceasedDateTime' in data.keys():   
            self.deceasedDateTime = datetime.datetime.fromisoformat(data['deceasedDateTime'])
            if self.deceasedDateTime.date() < today:
                self.isDeceased = True
            else:
                self.isDeceased = False
        else:
            self.isDeceased = False
            self.deceasedDateTime = None

    def __repr__(self):
        return "Patient(\n  "+"\n  ".join([f"{key} = {value}" for (key,value) in self.__dict__.items()])+"\n)"

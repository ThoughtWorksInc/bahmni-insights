from collections import defaultdict
from datetime import datetime
import MySQLdb
import pandas as pd
from flask import Flask, session, jsonify, Response, g, render_template, request

import json

# create our little application :)
app = Flask(__name__)

# Load default config and override config from an environment variable
app.config.update(dict(
    # DATABASE=os.path.join(app.root_path, 'flaskr.db'),
    DEBUG=True,
    SECRET_KEY='development key 918273645',
    # USERNAME='admin',
    # PASSWORD='default'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)

cache = {}


def _load_patient_demographics_and_disease():
    mysql_cn = MySQLdb.connect(host='localhost',
                               port=3306, user='root', passwd='',
                               db='openmrs')
    disease_data = pd.read_sql("""SELECT concept_name.name,concept_id,obs_date,patient_dob, patient_gender,city_village
    from (
           SELECT
             obs.concept_id              AS obs_concept,
             obs.value_coded             AS obs_value,
             obs.date_created            AS obs_date,
             person.birthdate            AS patient_dob,
             person.gender               AS patient_gender,
             person_address.city_village AS city_village
           FROM obs
             LEFT JOIN person ON obs.person_id = person.person_id
             LEFT JOIN person_address ON person.person_id = person_address.person_id
           WHERE
             obs.concept_id = 43
             AND obs.voided = 0
         )as obs_data
      LEFT JOIN concept_name on obs_value= concept_name.concept_id
    WHERE concept_name.concept_name_type = 'FULLY_SPECIFIED'""", con=mysql_cn)
    print 'loaded dataframe from MySQL. records:', len(disease_data)
    mysql_cn.close()

    disease_data['patient_dob'] = disease_data['patient_dob'].astype('datetime64')
    dd = pd.DataFrame()
    dd['disease_name'] = disease_data['name']
    dd['disease_code'] = disease_data['concept_id']
    dd['patient_age'] = disease_data['patient_dob'].apply(lambda x: (datetime.now() - x).days / 365)
    dd['date_observed'] = disease_data['obs_date']
    dd['gender'] = disease_data['patient_gender'].apply(lambda g: 0 if g == 'M' else 1)
    dd['place'] = disease_data['city_village']
    # dd['place'] = disease_data['city_village'].apply(lambda place: place.lower() if place else "")
    cache['patient_disease'] = dd
    return disease_data


@app.route('/patient-disease', methods=['GET'])
def get_patient_demographic_disease():
    if not cache.get('patient_disease'):
        print "huge data gathering..."
        _load_patient_demographics_and_disease()
    # json_data = session.patient_disease.to_json(orient='records')
    json_data = cache['patient_disease'].to_json(orient='records')
    # print "heres the data"
    # print json_data
    return json_data


@app.route('/disease-spread', methods=['GET'])
def get_disease_spread():
    pp = cache.get('patient_disease')
    if pp is None:
        print "huge data gathering..."
        _load_patient_demographics_and_disease()

    disease_df = cache['patient_disease']
    disease_grpd = disease_df[['disease_name', 'place']].groupby(['disease_name', 'place']).size()
    # disease_grpd = disease_df[['place','disease_name']].groupby(['place','disease_name']).size()
    disease_grpd_smaller = disease_grpd[disease_grpd > 2]
    return to_nested_json(disease_grpd_smaller)


@app.route('/disease-over-time', methods=['GET'])
def disease_over_time():
    if cache.get('patient_disease') is None:
        print "huge data gathering..."
        _load_patient_demographics_and_disease()

    disease_df = cache['patient_disease']
    disease_df['month'] = disease_df['date_observed'].apply(lambda x: x.month)

    disease_grpd = disease_df[['disease_name', 'month']].groupby(['disease_name', 'month']).size()
    disease_grpd_smaller = disease_grpd[disease_grpd > 0]
    return to_nested_json(disease_grpd_smaller)


def to_nested_json(group_by_result):
    results = defaultdict(lambda: defaultdict(dict))
    for indexes, value in group_by_result.iteritems():
        for key_index, key_value in enumerate(indexes):
            if key_index == 0:
                nested = results[key_value]
            else:
                nested[key_value] = value
    return json.dumps(results)


def top_n_diseases(data, n):
    diseases = data.groupby(['disease_name']).size()
    diseases.sort(ascending=False)
    return diseases[0:n]


def records_for_top_n_diseases(data, n):
    top_diseases = top_n_diseases(data, n)
    return data.loc[data['disease_name'].isin(top_diseases.index)]


@app.route('/hello', methods=['GET'])
def hello():
    return "hello"


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run()

from datetime import datetime
import MySQLdb
import pandas as pd
from flask import Flask,session, jsonify, Response,g,render_template
# import json

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

cache ={};
def _get_patient_demographics_and_disease():
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
    return disease_data


@app.route('/patient-disease', methods=['GET'])
def get_patient_demographic_disease():
    if not cache.get('patient_disease'):
        print "huge data gathering..."
        disease_data = _get_patient_demographics_and_disease()
        disease_data['patient_dob'] =disease_data['patient_dob'].astype('datetime64')
        dd = pd.DataFrame()
        dd['disease_name'] = disease_data['name']
        dd['disease_code'] = disease_data['concept_id']
        dd['patient_age'] = disease_data['patient_dob'].apply(lambda x: (datetime.now() - x).days / 365)
        dd['date_observed'] = disease_data['obs_date']
        dd['gender'] = disease_data['patient_gender'].apply(lambda g: 0 if g == 'M' else 1)
        dd['place'] = disease_data['city_village']
        cache['patient_disease'] = dd.to_json(orient='records')
    # json_data = session.patient_disease.to_json(orient='records')
    json_data = cache['patient_disease']
    # print "heres the data"
    # print json_data
    return json_data


@app.route('/hello', methods=['GET'])
def hello():
    return "hello"

@app.route('/',methods=['GET'])
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run()
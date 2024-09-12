#importing libraries
import boto3
from extract_txt import read_files
from txt_processing import preprocess
from txt_to_features import txt_features, feats_reduce
from extract_entities import get_number, get_email, rm_email, rm_number, get_name, get_skills
from model import simil
import pandas as pd
import json
import os
import uuid
import shutil
from flask import Flask, flash, request, redirect, url_for, render_template, send_file,jsonify
from dotenv import load_dotenv
load_dotenv()
#used directories for data, downloading and uploading files 
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files/resumes/')
DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files/outputs/')
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Data/')

# Make directory if UPLOAD_FOLDER does not exist
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

# Make directory if DOWNLOAD_FOLDER does not exist
if not os.path.isdir(DOWNLOAD_FOLDER):
    os.mkdir(DOWNLOAD_FOLDER)
#Flask app config 
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.config['DATA_FOLDER'] = DATA_FOLDER
app.config['SECRET_KEY'] = 'nani?!'
app.config['S3_BUCKET'] = os.getenv('S3_BUCKET')
app.config['S3_REGION'] = os.getenv('S3_REGION')
app.config['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
app.config['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
  

# Initialize Flask app



# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'doc', 'docx'])

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def download_file_from_s3(s3_key, download_path):
    s3 = boto3.client('s3',
                      region_name=app.config['S3_REGION'],
                      aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'])
    s3.download_file(app.config['S3_BUCKET'], s3_key, download_path)
# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'doc','docx'])

 
@app.route('/', methods=['GET'])
def main_page():
    return _show_page()
 
@app.route('/', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    app.logger.info(request.files)
    upload_files = request.files.getlist('file')
    app.logger.info(upload_files)
    # If the user does not select a file, the browser submits an
    # empty file without a filename.
    if not upload_files:
        flash('No selected file')
        return redirect(request.url)
    for file in upload_files:
        original_filename = file.filename
        if allowed_file(original_filename):
            extension = original_filename.rsplit('.', 1)[1].lower()
            filename = str(uuid.uuid1()) + '.' + extension
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            file_list = os.path.join(UPLOAD_FOLDER, 'files.json')
            files = _get_files()
            files[filename] = original_filename
            with open(file_list, 'w') as fh:
                json.dump(files, fh)
 
    flash('Upload succeeded')
    return redirect(url_for('upload_file'))
 
 
@app.route('/download/<code>', methods=['GET'])
def download(code):
    files = _get_files()
    if code in files:
        path = os.path.join(UPLOAD_FOLDER, code)
        if os.path.exists(path):
            return send_file(path)
    abort(404)
 
def _show_page():
    files = _get_files()
    return render_template('index.html', files=files)
 
def _get_files():
    file_list = os.path.join(UPLOAD_FOLDER, 'files.json')
    if os.path.exists(file_list):
        with open(file_list) as fh:
            return json.load(fh)
    return {}


@app.route('/process',methods=["POST"])
def process():
    if request.method == 'POST':

        rawtext = request.form['rawtext']
        jdtxt=[rawtext]
        resumetxt=read_files(UPLOAD_FOLDER)
        p_resumetxt = preprocess(resumetxt)
        p_jdtxt = preprocess(jdtxt)

        feats = txt_features(p_resumetxt, p_jdtxt)
        feats_red = feats_reduce(feats)

        df = simil(feats_red, p_resumetxt, p_jdtxt)

        t = pd.DataFrame({'Original Resume':resumetxt})
        dt = pd.concat([df,t],axis=1)

        dt['Phone No.']=dt['Original Resume'].apply(lambda x: get_number(x))
        
        dt['E-Mail ID']=dt['Original Resume'].apply(lambda x: get_email(x))

        dt['Original']=dt['Original Resume'].apply(lambda x: rm_number(x))
        dt['Original']=dt['Original'].apply(lambda x: rm_email(x))
        dt['Candidate\'s Name']=dt['Original'].apply(lambda x: get_name(x))

        skills = pd.read_csv(DATA_FOLDER+'skill_red.csv')
        skills = skills.values.flatten().tolist()
        skill = []
        for z in skills:
            r = z.lower()
            skill.append(r)

        dt['Skills']=dt['Original'].apply(lambda x: get_skills(x,skill))
        dt = dt.drop(columns=['Original','Original Resume'])
        sorted_dt = dt.sort_values(by=['JD 1'], ascending=False)

        out_path = DOWNLOAD_FOLDER+"Candidates.csv"
        sorted_dt.to_csv(out_path,index=False)

        return send_file(out_path, as_attachment=True)




@app.route('/process_s3', methods=["POST"])
def process_s3():
    try:
        if request.method == 'POST':
            data = request.json
            applicants = data.get('applicants')
            job_description = data.get('jobDescription')
            job_id = data.get('job_id')
            no_of_applicants = data.get('noOfApplicants')
            print(data)
            if not applicants or not job_description or not job_id or not no_of_applicants:
                return jsonify({"error": "Missing required fields"}), 400

            # Create a directory for the specific job using job_id
            job_folder = os.path.join(app.config['UPLOAD_FOLDER'], f"job_{job_id}")
            if not os.path.exists(job_folder):
                os.makedirs(job_folder)

            downloaded_files = []
            user_ids = []
            for applicant in applicants:
                s3_key = applicant.get('s3_key')
                user_id = applicant.get('user_id')

                if not s3_key or not user_id:
                    continue  # Skip invalid entries

                filename = f"{s3_key}.pdf"

                # Construct the full download path inside the job folder
                download_path = os.path.join(job_folder, filename)
            
                # Download the file from S3 and save it to the specified path
                download_file_from_s3(s3_key, download_path)
            
                # Append the path of the downloaded file and user_id
                downloaded_files.append(download_path)
                user_ids.append(user_id)

            # Read and process the downloaded resumes
            resumetxt = read_files(job_folder)
            p_resumetxt = preprocess(resumetxt)
            p_jdtxt = preprocess([job_description])

            feats = txt_features(p_resumetxt, p_jdtxt)
            feats_red = feats_reduce(feats)

            df = simil(feats_red, p_resumetxt, p_jdtxt)

            t = pd.DataFrame({'Original Resume': resumetxt, 'User ID': user_ids})
            dt = pd.concat([df, t], axis=1)

            dt['Phone No.'] = dt['Original Resume'].apply(get_number)
            dt['E-Mail ID'] = dt['Original Resume'].apply(get_email)
            dt['Original'] = dt['Original Resume'].apply(rm_number)
            dt['Original'] = dt['Original'].apply(rm_email)
            dt['Candidate\'s Name'] = dt['Original'].apply(get_name)

            skills = pd.read_csv(os.path.join(app.config['DATA_FOLDER'], 'skill_red.csv'))
            skills = skills.values.flatten().tolist()
            skill = [z.lower() for z in skills]

            dt['Skills'] = dt['Original'].apply(lambda x: get_skills(x, skill))
            dt = dt.drop(columns=['Original', 'Original Resume'])
            sorted_dt = dt.sort_values(by=['JD 1'], ascending=False)
            # Limit the number of shortlisted candidates to no_of_applicants
            top_candidates = sorted_dt.head(no_of_applicants)

            # Extract top applicants' user IDs
            top_applicants = top_candidates['User ID'].tolist()

            # Clean up: remove the job-specific folder after processing
            shutil.rmtree(job_folder)

            return jsonify({"topApplicants": top_applicants})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)


